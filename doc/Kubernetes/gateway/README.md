# Kubernetes Gateway API with Envoy

## Goal

Expose OrderService and InventoryService through one entry point.

Instead of using separate port-forwards:

- OrderService: `localhost:8087`
- InventoryService: `localhost:8086`

both APIs are now accessed through:

- Gateway: `localhost:8088`

## Request flow

Browser or Postman
↓
Envoy Gateway
↓
HTTPRoute checks the URL
├── `/api/orders` → OrderService
└── `/api/inventory` → InventoryService

## Main components

### 1. Helm

Helm is a package manager for Kubernetes.

It installs complex Kubernetes software using a prepared package called a Helm chart.

Installed version:

```text
v4.1.4|

We used Helm to install Envoy Gateway.

2. Envoy Gateway controller
The Envoy Gateway controller watches Kubernetes Gateway API resources.When it sees a Gateway and HTTPRoute, it creates and configures an Envoy proxy
that handles incoming requests. Installed version: v1.9.2

Installed using:
> helm install eg oci://docker.io/envoyproxy/gateway-helm `
  --version v1.9.2 `
  --namespace envoy-gateway-system `
  --create-namespace

Verify the controller:
> kubectl get pods -n envoy-gateway-system

3. GatewayClass
File: k8s/envoy-gateway-class.yaml

The GatewayClass tells Kubernetes which controller manages a Gateway. Our GatewayClass uses: controllerName: gateway.envoyproxy.io/gatewayclass-controller 
Simple meaning: Use Envoy to manage Gateways that belong to this class.
Verify: kubectl get gatewayclass
Expected: ACCEPTED=True

4. Gateway
File: k8s/microservices-gateway.yaml
The Gateway creates the entrance for incoming HTTP traffic.
It uses: gatewayClassName: envoy-gateway-class

It listens for HTTP traffic on port 80.
Verify: kubectl get gateway

Expected: PROGRAMMED=True
This means Envoy successfully configured the Gateway.

5. HTTPRoute
File: k8s/microservices-routes.yaml
The HTTPRoute examines the URL path and selects the correct Kubernetes Service.
Routing rules: 
URL-path		Kubernetes Service
/api/orders		orderservice-service
/api/inventory	inventoryservice-service


Verify: kubectl describe httproute microservices-routes

Expected conditions: Accepted=True
ResolvedRefs=True

Meanings:
- Accepted=True: the Gateway accepted the route.
- ResolvedRefs=True: Kubernetes found the referenced Services.

Local access:
The Envoy proxy Service was forwarded to local port 8088:
kubectl port-forward `
  -n envoy-gateway-system `
  service/envoy-default-microservices-gateway-63094d12 `
  8088:80

The generated Service name may change if the Gateway is recreated. Find it with:
kubectl get services --all-namespaces |
  Select-String "envoy"

Keep the port-forward terminal open while testing.
Testing
OrderService:
GET http://localhost:8088/api/orders
POST http://localhost:8088/api/orders

InventoryService:
GET http://localhost:8088/api/inventory

