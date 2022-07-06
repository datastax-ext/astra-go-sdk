package astra

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type TestContainer struct {
	grpcEndpoint string
	authEndpoint string
}

func NewStargateTestContainer() (*TestContainer, error) {
	ctx := context.Background()
	waitStrategy := wait.ForHTTP("/checker/readiness").WithPort("8084/tcp").WithStartupTimeout(90 * time.Second)

	req := testcontainers.ContainerRequest{
		Image: "stargateio/stargate-3_11:v1.0.40",
		Env: map[string]string{
			"CLUSTER_NAME":    "test",
			"CLUSTER_VERSION": "3.11",
			"DEVELOPER_MODE":  "true",
			"ENABLE_AUTH":     "true",
		},
		ExposedPorts: []string{"8090/tcp", "8081/tcp", "8084/tcp", "9042/tcp"},
		WaitingFor:   waitStrategy,
	}

	stargateContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to start Stargate container: %w", err)
	}

	grpcPort, err := nat.NewPort("tcp", "8090")
	if err != nil {
		return nil, fmt.Errorf("failed to get port: %w", err)
	}
	authPort, err := nat.NewPort("tcp", "8081")
	if err != nil {
		return nil, fmt.Errorf("failed to get port: %w", err)
	}

	stc := &TestContainer{}

	stc.grpcEndpoint, err = stargateContainer.PortEndpoint(ctx, grpcPort, "")
	if err != nil {
		return nil, fmt.Errorf("failed to get endpoint: %w", err)
	}

	stc.authEndpoint, err = stargateContainer.PortEndpoint(ctx, authPort, "")
	if err != nil {
		return nil, fmt.Errorf("failed to get endpoint: %w", err)
	}
	return stc, nil
}

func (stc *TestContainer) CreateClientWithStaticToken() (*Client, error) {
	token, err := stc.getAuthToken()
	if err != nil {
		return nil, fmt.Errorf("failed to get auth token: %w", err)
	}

	c, err := NewStaticTokenClient(stc.grpcEndpoint, token)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize client: %w", err)
	}

	return c, nil
}

func (stc *TestContainer) getAuthToken() (string, error) {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		fmt.Sprintf("http://%s/v1/auth", stc.authEndpoint),
		strings.NewReader("{\"username\": \"cassandra\",\"password\": \"cassandra\"}"),
	)
	if err != nil {
		return "", fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Add("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error calling auth service: %w", err)
	}

	defer func() {
		err := response.Body.Close()
		if err != nil {
			log.Printf("unable to close response body: %v", err)
		}
	}()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("error reading response body: %w", err)
	}

	var result map[string]string
	err = json.Unmarshal(body, &result)
	if err != nil {
		return "", fmt.Errorf("error unmarshalling response body: %w", err)
	}

	return result["authToken"], nil
}
