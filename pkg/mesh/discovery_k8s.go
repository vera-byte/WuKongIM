package wkmesh

// import (
// 	"context"
// 	"fmt"
// 	"strings"
// 	"time"

// 	"github.com/WuKongIM/WuKongIM/version"
// 	"go.uber.org/zap"
// 	corev1 "k8s.io/api/core/v1"
// 	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
// 	"k8s.io/client-go/kubernetes"
// 	"k8s.io/client-go/rest"
// )

// func (d *Discovery) initK8s() {
// 	d.Log.Info("运行在Kubernetes环境中")

// 	// 初始化 Kubernetes 客户端
// 	if err := d.initK8sClient(); err != nil {
// 		d.Log.Error("Kubernetes客户端初始化失败", zap.Error(err))
// 	} else {
// 		// 启动 Kubernetes 发现循环
// 		go d.k8sDiscoveryLoop()

// 		// 立即执行一次发现
// 		go d.discoverK8sPods()
// 	}

// 	// 启动单播循环
// 	go d.unicastLoop()
// }

// // 初始化 Kubernetes 客户端
// func (d *Discovery) initK8sClient() error {
// 	// 创建集群内配置
// 	config, err := rest.InClusterConfig()
// 	if err != nil {
// 		return fmt.Errorf("创建集群内配置失败: %w", err)
// 	}

// 	// 创建客户端
// 	clientset, err := kubernetes.NewForConfig(config)
// 	if err != nil {
// 		return fmt.Errorf("创建Kubernetes客户端失败: %w", err)
// 	}

// 	d.k8sClient = clientset
// 	d.Log.Info("Kubernetes客户端初始化成功")
// 	return nil
// }

// // 动态发现 Kubernetes Pod
// func (d *Discovery) discoverK8sPods() {
// 	if d.k8sClient == nil {
// 		return
// 	}

// 	// 获取命名空间
// 	namespace := getEnv("POD_NAMESPACE", "default")
// 	if namespace == "" {
// 		namespace = "default"
// 	}

// 	// 获取标签选择器
// 	selector := getEnv("MESH_POD_SELECTOR", "app=wukong-im")

// 	d.Log.Debug("发现Kubernetes Pod",
// 		zap.String("namespace", namespace),
// 		zap.String("selector", selector),
// 	)

// 	// 获取 Pod 列表
// 	pods, err := d.k8sClient.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
// 		LabelSelector: selector,
// 	})
// 	if err != nil {
// 		d.Log.Error("获取Pod列表失败", zap.Error(err))
// 		return
// 	}

// 	d.Log.Info("发现Kubernetes Pod", zap.Int("count", len(pods.Items)))

// 	// 构建当前存在的Pod名称集合
// 	currentPodNames := make(map[string]struct{})
// 	for _, pod := range pods.Items {
// 		currentPodNames[pod.Name] = struct{}{}
// 	}

// 	// 找出需要标记为下线的节点
// 	var nodesToMarkOffline []string
// 	d.mu.RLock()
// 	for name, node := range d.nodes {
// 		fmt.Println(node)
// 		// 跳过静态节点和自身节点
// 		if strings.HasPrefix(name, "static-") || name == d.SelfName {
// 			continue
// 		}
// 		// 如果该节点不在当前Pod列表中，标记为下线
// 		if _, exists := currentPodNames[name]; !exists {
// 			nodesToMarkOffline = append(nodesToMarkOffline, name)
// 		}
// 	}
// 	d.mu.RUnlock()

// 	// 标记节点为下线
// 	for _, name := range nodesToMarkOffline {
// 		d.mu.Lock()
// 		if node, exists := d.nodes[name]; exists {
// 			node.Status = NodeStatusOffline
// 			node.LastSeen = time.Now()
// 			d.nodes[name] = node
// 			d.Log.Info("标记Kubernetes节点为下线", zap.String("name", name))
// 		}
// 		d.mu.Unlock()
// 	}

// 	// 处理发现的 Pod
// 	for _, pod := range pods.Items {
// 		// 跳过自身
// 		if pod.Status.PodIP == GetLocalIP() {
// 			continue
// 		}

// 		// 跳过非运行状态的 Pod
// 		if pod.Status.Phase != corev1.PodRunning {
// 			d.Log.Debug("跳过非运行状态Pod",
// 				zap.String("name", pod.Name),
// 				zap.String("phase", string(pod.Status.Phase)),
// 			)
// 			continue
// 		}

// 		// 获取端口
// 		port := getEnv("MESH_PORT", "11110")

// 		// 检查容器中是否有自定义端口设置
// 		if len(pod.Spec.Containers) > 0 {
// 			for _, env := range pod.Spec.Containers[0].Env {
// 				if env.Name == "MESH_PORT" {
// 					port = env.Value
// 					break
// 				}
// 			}
// 		}

// 		// 添加或更新节点
// 		d.addOrUpdateNode(NodeInfo{
// 			Name:      pod.Name,
// 			IP:        pod.Status.PodIP,
// 			Port:      port,
// 			Version:   version.Version,
// 			LastSeen:  time.Now(),
// 			Reachable: true, // 假设可达，后续会检查
// 			Status:    NodeStatusOnline,
// 		})
// 	}
// }

// // 定期发现循环
// func (d *Discovery) k8sDiscoveryLoop() {
// 	d.Log.Info("启动Kubernetes发现循环", zap.String("interval", "30s"))

// 	ticker := time.NewTicker(30 * time.Second)
// 	defer ticker.Stop()

// 	for !d.shutdown {
// 		<-ticker.C
// 		d.discoverK8sPods()
// 	}
// }
