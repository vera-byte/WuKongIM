package wk_kubernetes

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type WKKubernetesClient struct {
	clientset     *kubernetes.Clientset
	namespace     string
	selector      string
	nodeAddresses map[string]string // podName => IP:port
	mu            sync.RWMutex
}

func NewWKKubernetesClient(namespace, selector string) (*WKKubernetesClient, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("加载集群配置失败: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("创建 clientset 失败: %w", err)
	}

	return &WKKubernetesClient{
		clientset:     clientset,
		namespace:     namespace,
		selector:      selector,
		nodeAddresses: make(map[string]string),
	}, nil
}

// StartWatchingPods 启动监听
func (w *WKKubernetesClient) StartWatchingPods(ctx context.Context, port int) {
	factory := informers.NewSharedInformerFactoryWithOptions(
		w.clientset,
		0, // 不缓存
		informers.WithNamespace(w.namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = w.selector
		}),
	)

	informer := factory.Core().V1().Pods().Informer()

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*v1.Pod)
			w.addPod(pod, port)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			pod := newObj.(*v1.Pod)
			w.addPod(pod, port)
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*v1.Pod)
			w.removePod(pod)
		},
	})

	go informer.Run(ctx.Done())
	log.Println("已启动 wukongim pod 监听器")
}

// 添加 Pod 到列表
func (w *WKKubernetesClient) addPod(pod *v1.Pod, port int) {
	if pod.Status.PodIP == "" {
		return
	}
	addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, port)

	w.mu.Lock()
	w.nodeAddresses[pod.Name] = addr
	w.mu.Unlock()

	log.Printf("[Add/Update] %s -> %s\n", pod.Name, addr)
}

// 删除 Pod
func (w *WKKubernetesClient) removePod(pod *v1.Pod) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.nodeAddresses, pod.Name)
	log.Printf("[Delete] 移除节点: %s\n", pod.Name)
}

// 获取当前所有节点地址列表
func (w *WKKubernetesClient) ListNodeAddresses() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var list []string
	for _, addr := range w.nodeAddresses {
		list = append(list, addr)
	}
	return list
}

func init() {
	ctx := context.Background()

	k8sClient, err := NewWKKubernetesClient("default", "app=wukongim")
	if err != nil {
		log.Fatalf("初始化失败: %v", err)
	}

	k8sClient.StartWatchingPods(ctx, 8000) // TODO：替换为实际端口号

	// 每隔几秒打印一次节点列表
	go func() {
		for {
			addresses := k8sClient.ListNodeAddresses()
			log.Printf("当前集群节点列表: %v", addresses)
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}
		}
	}()

	<-ctx.Done() // 持续运行
}
