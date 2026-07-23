package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/KitHub/kitdb/component"
	"github.com/KitHub/kitdb/config"
	servicecontext "github.com/KitHub/kitdb/servicecontext"

	"github.com/KitHub/protocols/kitdb"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type ServerArgs struct {
	ConfigFile string
}

func main() {
	ctx := context.Background()
	args := parepareArgs(ctx)

	// init config
	slog.InfoContext(ctx, "init config",
		slog.String("config_file", args.ConfigFile))
	configEntity, err := config.LoadConfig(ctx, args.ConfigFile)
	if err != nil {
		slog.ErrorContext(ctx, "init config failed",
			slog.String("error", err.Error()))
		panic(err)
	}
	slog.InfoContext(ctx, "init config done")

	// init service context
	serviceContext, err := servicecontext.InitServiceContext(
		ctx, &configEntity)
	if err != nil {
		slog.ErrorContext(ctx, "failed to init service context",
			slog.String("error", err.Error()))
		panic(err)
	}

    // init callbacks
	err = initServer(ctx, serviceContext.InitComponent.GetInitCallbacks(ctx))
	if err != nil {
		slog.ErrorContext(ctx, "failed to init server", slog.String("error", err.Error()))
		panic(err)
	}

	// init services
	err = initServices(ctx, &configEntity, serviceContext)
	if err != nil {
		slog.ErrorContext(ctx, "failed to init services",
			slog.String("error", err.Error()))
		panic(err)
	}

	shutdownGracefully(ctx, servicecontext.GetServiceContext().ShutdownComponent.GetShutdownCallbacks(ctx))
}

func parepareArgs(ctx context.Context) ServerArgs {
	configFile := flag.String("server_config", "", "config file for server")
	flag.Parse()
	result := ServerArgs{
		ConfigFile: *configFile,
	}
	slog.InfoContext(ctx, "parse flags done")
	return result
}

func initServices(ctx context.Context, serviceConfig *config.ConfigEntity,
	serviceContext *servicecontext.ServiceContext) (err error) {

	slog.InfoContext(ctx, "init services")
	grpcServiceConfig := serviceConfig.ServerConfig.GrpcService
	httpServiceConfig := serviceConfig.ServerConfig.HttpService
	_, err = initRpcServer(ctx, grpcServiceConfig, serviceContext)
	if err != nil {
		slog.ErrorContext(ctx, "failed to init RPC server",
			slog.String("error", err.Error()))
		return err
	}
	_, err = initHttpServer(ctx, httpServiceConfig, grpcServiceConfig, serviceContext)
	if err != nil {
		slog.ErrorContext(ctx, "failed to init HTTP server",
			slog.String("error", err.Error()))
		return err
	}
	slog.InfoContext(ctx, "init services done")
	return nil
}

// initRpcServer initializes the gRPC server and registers the service implementation.
func initRpcServer(ctx context.Context, serverConfig *config.ServiceConfigEntity, serviceContext *servicecontext.ServiceContext) (*grpc.Server, error) {
	slog.InfoContext(ctx, "init rpc server", slog.Any("serverConfig", serverConfig))
	hostAndPort := fmt.Sprintf("%s:%d", serverConfig.Host, serverConfig.Port)
	listener, err := net.Listen("tcp", hostAndPort)
	if err != nil {
		slog.ErrorContext(ctx, "failed to listen",
			slog.String("error", err.Error()))
		return nil, err
	}
	// create a new gRPC server
	server := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	// bind the service implementation to the gRPC server
	kitdb.RegisterDemoAPIServer(
		server, serviceContext.DemoService)

	go func() {
		err := server.Serve(listener)
		if err != nil {
			slog.ErrorContext(ctx, "failed to serve",
				slog.String("error", err.Error()))
			panic(err)
		}
	}()

	servicecontext.GetServiceContext().ShutdownComponent.RegisterShutdownCallback(func(ctx context.Context) error {
		server.GracefulStop()
		slog.InfoContext(ctx, "gRPC server stopped gracefully")
		return nil
	})

	return server, nil
}


// initHttpServer initializes the HTTP server and registers the http gateway.
// The http gateway translates HTTP API into gRPC calls to the gRPC server.
func initHttpServer(ctx context.Context, httpServerConfig *config.ServiceConfigEntity, grpcServerConfig *config.ServiceConfigEntity, serviceContext *servicecontext.ServiceContext) (*http.Server, error) {
	slog.InfoContext(ctx, "init http server", slog.Any("serverConfig", httpServerConfig))
	grpcHostAndPort := fmt.Sprintf("%s:%d", grpcServerConfig.Host, grpcServerConfig.Port)
	httpHostAndPort := fmt.Sprintf("%s:%d", httpServerConfig.Host, httpServerConfig.Port)
	gateway := runtime.NewServeMux(runtime.WithForwardResponseOption(rspModifier))
	err := kitdb.RegisterDemoAPIHandlerFromEndpoint(ctx, gateway, grpcHostAndPort, []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())})
	if err != nil {
		slog.ErrorContext(ctx, "Failed to register http gateway", slog.String("error", err.Error()))
		return nil, err
	}

	server := http.Server{
		Addr:    httpHostAndPort,
		Handler: corsMiddleware(gateway),
	}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				slog.InfoContext(ctx, "HTTP server closed")
				return
			} else {
				slog.ErrorContext(ctx, "Failed to start HTTP gateway", slog.String("error", err.Error()))
				panic(err)
			}
		}
	}()

	servicecontext.GetServiceContext().ShutdownComponent.RegisterShutdownCallback(func(ctx context.Context) error {
		err = server.Shutdown(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to shutdown HTTP server gracefully", slog.String("error", err.Error()))
			return err
		}
		slog.InfoContext(ctx, "HTTP server stopped gracefully")
		return nil
	})

	return &server, nil
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// allow all origins
		w.Header().Set("Access-Control-Allow-Origin", "*")
		// allow the specified headers
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept, Authorization")
		// allow the specified methods
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Max-Age", "3600")
		w.Header().Set("Access-Control-Expose-Headers", "Content-Disposition")

		// handle preflight requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// 网关WithForwardResponseOption，统一拦截HttpBody设置下载头
func rspModifier(ctx context.Context, w http.ResponseWriter, resp proto.Message) error {
	_, ok := resp.(*httpbody.HttpBody)
	if !ok {
		slog.DebugContext(ctx, "response is not of type HttpBody, cannot set download header")
		return nil
	}

	smd, ok := runtime.ServerMetadataFromContext(ctx)
	if !ok {
		slog.ErrorContext(ctx, "failed to get server metadata from context")
		return nil
	}

	fnList := smd.HeaderMD.Get("download-filename")
	if len(fnList) == 0 {
		return nil
	}
	fileName := fnList[0]
	enc := url.QueryEscape(fileName)

	disp := fmt.Sprintf(`attachment; filename="%s"; filename*=UTF-8''%s`, enc, enc)
	w.Header().Set("Content-Disposition", disp)
	return nil
}

func shutdownGracefully(ctx context.Context, shutdownCallbacks []component.ShutdownCallback) {
	slog.InfoContext(ctx, "listening close signals...")
	c := make(chan os.Signal, 1)
	signal.Notify(
		c, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT,
		syscall.SIGTERM,
	)

	<-c
	slog.InfoContext(ctx, "graceful shutdown being executed...")

	for _, callback := range shutdownCallbacks {
		if err := callback(ctx); err != nil {
			slog.ErrorContext(ctx, "failed to execute shutdown callback", slog.Any("error", err))
		}
	}

	slog.InfoContext(ctx, "graceful shutdown done")
}


func initServer(ctx context.Context, initCallbacks []component.InitCallback) error {
	slog.InfoContext(ctx, "start init callbacks")
	for _, callback := range initCallbacks {
		if err := callback(ctx); err != nil {
			slog.ErrorContext(ctx, "failed to execute init callback", slog.Any("error", err))
			return err
		}
	}
	return nil
}
