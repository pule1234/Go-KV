package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/tidwall/redcon"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"rose"
	"rose/logger"
	"sync"
	"syscall"
	"time"
)

var (
	errClientIsNil = errors.New("ERR client conn is nil")
)

var (
	defaultDBPath            = filepath.Join("tmp", "rosedb")
	defaultHost              = "127.0.0.1"
	defaultPort              = "5200"
	defaultDatabasesNum uint = 16
)

const (
	dbName = "rose-%04d"
)

func init() {
	path, _ := filepath.Abs("resource/banner.txt")
	banner, _ := ioutil.ReadFile(path)
	fmt.Println(string(banner))
}

type Server struct {
	dbs    map[int]*rose.RoseDB
	ser    *redcon.Server
	signal chan os.Signal
	opts   ServerOptions
	mu     *sync.RWMutex
}

type ServerOptions struct {
	dbpath    string
	host      string
	port      string
	databases uint
}

func main() {
	// init server options
	serverOpts := new(ServerOptions)
	flag.StringVar(&serverOpts.dbpath, "dbpath", defaultDBPath, "db path")
	flag.StringVar(&serverOpts.host, "host", defaultHost, "server host")
	flag.StringVar(&serverOpts.port, "port", defaultPort, "server port")
	flag.UintVar(&serverOpts.databases, "databases", defaultDatabasesNum, "the number of databases")
	flag.Parse()

	// open a default databases
	path := filepath.Join(serverOpts.dbpath, fmt.Sprintf(dbName, 0))
	opts := rose.DefaultOptions(path)
	now := time.Now()
	//创建一个db实例
	db, err := rose.Open(opts)
	if err != nil {
		logger.Errorf("open rosedb err, fail to start server. %v", err)
		return
	}
	logger.Infof("open db from [%s] successfully, time cost: %v", serverOpts.dbpath, time.Since(now))

	sig := make(chan os.Signal, 1)
	// 获取信号
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	dbs := make(map[int]*rose.RoseDB)
	dbs[0] = db

	// init and start server
	svr := &Server{dbs: dbs, signal: sig, opts: *serverOpts, mu: new(sync.RWMutex)}
	addr := svr.opts.host + ":" + svr.opts.port
	// 创建redcon服务器实例 execClientCommand：用于执行客户端发送的命令         svr.redconAccept, ： 处理客户端连接请求
	redServer := redcon.NewServerNetwork("tcp", addr, execClientCommand, svr.redconAccept,
		func(conn redcon.Conn, err error) {
		})
	svr.ser = redServer
	go svr.listen()
	<-svr.signal
	svr.stop()
}

func (svr *Server) listen() {
	logger.Infof("rosedb server is running, ready to accept connections")
	if err := svr.ser.ListenAndServe(); err != nil {
		logger.Fatalf("listen and serve err, fail to start. %v", err)
		return
	}
}

func (svr *Server) stop() {
	for _, db := range svr.dbs {
		if err := db.Close(); err != nil {
			logger.Errorf("close db err: %v", err)
		}
	}
	if err := svr.ser.Close(); err != nil {
		logger.Errorf("close server err: %v", err)
	}
	logger.Infof("rosedb is ready to exit, bye bye...")
}

// 处理客户端连接请求
func (svr *Server) redconAccept(conn redcon.Conn) bool {
	cli := new(Client)
	cli.svr = svr
	svr.mu.RLock()
	cli.db = svr.dbs[0]
	svr.mu.RUnlock()
	conn.SetContext(cli)
	return true
}
