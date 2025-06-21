package wk_kubernetes

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// NodeChangeListener 是节点变更监听器接口
// 实现者可用于监听 Kubernetes Pod 的增删事件
// 用于动态服务发现、连接池管理、Raft 集群同步等

type NodeChangeListener interface {
	OnNodeUpdate(podName, address string) // Pod 添加或更新事件
	OnNodeDelete(podName string)          // Pod 删除事件
}

// WKKubernetesClient 是基于 Kubernetes 的服务发现客户端
// 它会持续监听符合标签的 Pod，并维护一张 Pod 实例映射表
// 同时支持连通性测试和外部事件通知（监听器）

type WKKubernetesClient struct {
	clientset *kubernetes.Clientset // Kubernetes API 客户端
	namespace string                // 监听的命名空间
	selector  string                // 标签选择器
	port      int                   // 节点服务端口
	nodes     map[string]*v1.Pod    // podName -> Pod 映射表
	mu        sync.RWMutex          // 并发锁，保护 nodess
	listeners []NodeChangeListener  // 外部注册的变更监听器
	wklog.Log                       // 日志记录器
}

var (
	wlog            = wklog.NewWKLog("WKKubernetes")
	GlobalK8sClient *WKKubernetesClient
)

// NewWKKubernetesClient 创建一个 Kubernetes 服务发现客户端
// 支持本地 kubeconfig 和集群内配置自动识别
func NewWKKubernetesClient(namespace, selector string, port int) (*WKKubernetesClient, error) {
	var config *rest.Config
	var err error

	config, err = rest.InClusterConfig()
	if config != nil || err == nil {
		wlog.Info("使用集群内配置 InClusterConfig")
	} else {
		wlog.Info("尝试使用kubeconfig")
		// 设置默认 kubeconfig 路径（开发环境）
		if os.Getenv("KUBECONFIG") == "" {
			homeDir, _ := os.UserHomeDir()
			defaultKubeconfig := filepath.Join(homeDir, ".kube", "config")
			_ = os.Setenv("KUBECONFIG", defaultKubeconfig)
			wlog.Info("未设置 KUBECONFIG，使用默认路径", zap.String("default", defaultKubeconfig))
		}

		if kubeconfigPath := os.Getenv("KUBECONFIG"); kubeconfigPath != "" {
			config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
			if err != nil {
				wlog.Error("加载 kubeconfig 文件失败", zap.String("path", kubeconfigPath), zap.Error(err))
				return nil, err
			}
			wlog.Info("使用本地 kubeconfig", zap.String("path", kubeconfigPath))
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("创建 clientset 失败: %w", err)
	}

	return &WKKubernetesClient{
		Log:       wlog,
		clientset: clientset,
		namespace: namespace,
		selector:  selector,
		port:      port,
		nodes:     make(map[string]*v1.Pod),
		listeners: make([]NodeChangeListener, 0),
	}, nil
}

// RegisterListener 注册一个监听器，当 Pod 增删事件发生时会调用
func (w *WKKubernetesClient) RegisterListener(listener NodeChangeListener) {
	w.listeners = append(w.listeners, listener)
}

// StartWatchingPods 启动 Pod 监听器
func (w *WKKubernetesClient) StartWatchingPods(ctx context.Context) {
	w.Info("启动 Pod 监听器", zap.String("namespace", w.namespace), zap.String("selector", w.selector))

	factory := informers.NewSharedInformerFactoryWithOptions(
		w.clientset,
		0,
		informers.WithNamespace(w.namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = w.selector
		}),
	)

	informer := factory.Core().V1().Pods().Informer()

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { w.addPod(obj.(*v1.Pod)) },
		UpdateFunc: func(_, newObj interface{}) { w.addPod(newObj.(*v1.Pod)) },
		DeleteFunc: func(obj interface{}) { w.removePod(obj.(*v1.Pod)) },
	})

	go informer.Run(ctx.Done())
	w.Info("Pod informer 启动成功")
}

// addPod 处理 Pod 新增或变更
func (w *WKKubernetesClient) addPod(pod *v1.Pod) {
	if pod.Status.PodIP == "" {
		return
	}
	w.mu.Lock()
	w.nodes[pod.Name] = pod
	w.mu.Unlock()

	addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, w.port)
	for _, listener := range w.listeners {
		go listener.OnNodeUpdate(pod.Name, addr)
	}
}

// removePod 处理 Pod 删除
func (w *WKKubernetesClient) removePod(pod *v1.Pod) {
	w.mu.Lock()
	delete(w.nodes, pod.Name)
	w.mu.Unlock()

	for _, listener := range w.listeners {
		go listener.OnNodeDelete(pod.Name)
	}
}

// GetPod 获取指定名称的 Pod
func (w *WKKubernetesClient) GetPod(podName string) (*v1.Pod, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	pod, ok := w.nodes[podName]
	return pod, ok
}

// GetNodeMap 返回 podName → 地址 的映射
func (w *WKKubernetesClient) GetNodeMap() map[string]string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	copyMap := make(map[string]string)
	for k, v := range w.nodes {
		if v.Status.PodIP != "" {
			copyMap[k] = fmt.Sprintf("%s:%d", v.Status.PodIP, w.port)
		}
	}
	return copyMap
}

// ListNodeAddresses 返回所有当前节点的地址（ip:port）
func (w *WKKubernetesClient) ListNodeAddresses() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var list []string
	for _, pod := range w.nodes {
		if pod.Status.PodIP != "" {
			list = append(list, fmt.Sprintf("%s:%d", pod.Status.PodIP, w.port))
		}
	}
	return list
}

// GetNodeAddressByName 根据 Pod 名称获取地址
func (w *WKKubernetesClient) GetNodeAddressByName(podName string) (string, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	pod, ok := w.nodes[podName]
	if !ok || pod.Status.PodIP == "" {
		return "", false
	}
	return fmt.Sprintf("%s:%d", pod.Status.PodIP, w.port), true
}

// isPodReady 判断 Pod 是否处于 Ready 状态（所有条件满足）
func isPodReady(pod *v1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == v1.PodReady && cond.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

// TestNodeConnectivity 检测所有节点是否可连接，并输出状态与延迟
func (w *WKKubernetesClient) TestNodeConnectivity(timeout time.Duration) map[string]NodeConnectionResult {
	results := make(map[string]NodeConnectionResult)
	w.mu.RLock()
	nodes := make(map[string]*v1.Pod)
	for k, v := range w.nodes {
		nodes[k] = v
	}
	w.mu.RUnlock()

	var wg sync.WaitGroup
	var mu sync.Mutex
	for name, pod := range nodes {
		if pod.Status.PodIP == "" {
			continue
		}
		address := fmt.Sprintf("%s:%d", pod.Status.PodIP, w.port)

		wg.Add(1)
		go func(podName string, pod *v1.Pod, addr string) {
			defer wg.Done()
			start := time.Now()
			conn, err := net.DialTimeout("tcp", addr, timeout)
			latency := time.Since(start)
			reachable := err == nil
			if conn != nil {
				conn.Close()
			}

			mu.Lock()
			results[podName] = NodeConnectionResult{
				Reachable: reachable,
				Latency:   latency,
				Phase:     string(pod.Status.Phase),
				Ready:     isPodReady(pod),
				Status:    string(pod.Status.Phase),
			}
			mu.Unlock()
		}(name, pod, address)
	}
	wg.Wait()
	return results
}

// NodeConnectionResult 表示单个节点的连通性检查结果
// 包括是否能连通，以及连接耗时

type NodeConnectionResult struct {
	Reachable bool          // 是否可连接
	Latency   time.Duration // 延迟（连接耗时）
	Phase     string        // Pod 当前状态（Pending/Running/Failed 等）
	Ready     bool          // 是否处于 Ready 状态
	Status    string        // Pod 状态描述（如 "ContainerCreating", "Running" 等）
}

// StartWukongimServiceDiscovery 启动服务发现
// 调用方通常在 main() 中启动此方法，并注册监听器以接收变更通知
func StartWukongimServiceDiscovery(namespace, selector string, port int, listener NodeChangeListener) {
	go func() {
		ctx := context.Background()

		client, err := NewWKKubernetesClient(namespace, selector, port)
		if err != nil {
			wlog.Warn("K8s 服务发现初始化失败", zap.Error(err))
			return
		}
		GlobalK8sClient = client

		if listener != nil {
			client.RegisterListener(listener)
		}

		client.StartWatchingPods(ctx)

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				client.Info("服务发现监听已停止")
				return
			case <-ticker.C:
				addresses := client.ListNodeAddresses()
				client.Info("当前节点列表", zap.Int("数量", len(addresses)), zap.Strings("addresses", addresses))
				connectivity := client.TestNodeConnectivity(2 * time.Second)
				for pod, result := range connectivity {
					client.Info("连接测试", zap.String("pod", pod), zap.String("状态", result.Status), zap.Bool("连通性", result.Reachable), zap.Duration("延迟", result.Latency))
				}
			}
		}
	}()
}
