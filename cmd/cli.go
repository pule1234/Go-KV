package main

import (
	"github.com/tidwall/redcon"
	"rose"
	"strings"
)

type cmdHandler func(cli *Client, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	// string commands
	"set":      set,
	"get":      get,
	"mget":     mGet,
	"getrange": getRange,
	"getdel":   getDel,
	"setex":    setEX,
	"setnx":    setNX,
	"mset":     mSet,
	"msetnx":   mSetNX,
	"append":   appendStr,
	"decr":     decr,
	"decrby":   decrBy,
	"incr":     incr,
	"incrby":   incrBy,
	"strlen":   strLen,

	// list
	"lpush":  lPush,
	"lpushx": lPushX,
	"rpush":  rPush,
	"rpushx": rPushX,
	"lpop":   lPop,
	"rpop":   rPop,
	"lmove":  lMove,
	"llen":   lLen,
	"lindex": lIndex,
	"lset":   lSet,
	"lrange": lRange,
	"lrem":   lRem,

	// hash commands
	"hset":       hSet,
	"hsetnx":     hSetNX,
	"hget":       hGet,
	"hmget":      hmGet,
	"hdel":       hDel,
	"hexists":    hExists,
	"hlen":       hLen,
	"hkeys":      hKeys,
	"hvals":      hVals,
	"hgetall":    hGetAll,
	"hstrlen":    hStrLen,
	"hscan":      hScan,
	"hincrby":    hIncrBy,
	"hrandfield": hRandField,

	// set commands
	"sadd":        sAdd,
	"spop":        sPop,
	"srem":        sRem,
	"sismember":   sIsMember,
	"smismember":  sMisMember,
	"smembers":    sMembers,
	"scard":       sCard,
	"sdiff":       sDiff,
	"sdiffstore":  sDiffStore,
	"sunion":      sUnion,
	"sunionstore": sUnionStore,
	"sinter":      sInter,
	"sinterstore": sInterStore,

	// zset commands
	"zadd":      zAdd,
	"zscore":    zScore,
	"zrem":      zRem,
	"zcard":     zCard,
	"zrange":    zRange,
	"zrevrange": zRevRange,
	"zrank":     zRank,
	"zrevrank":  zRevRank,

	// generic commands
	"type": keyType,
	"del":  del,

	// connection management commands
	"select": selectDB,
	"ping":   ping,
	"quit":   nil,

	// server management commands
	"info": info,
}

type Client struct {
	svr *Server
	db  *rose.RoseDB
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	// 将命令转换成小写
	command := strings.ToLower(string(cmd.Args[0]))
	//获取对应的处理函数
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("ERR unsupported command '" + string(cmd.Args[0]) + "'")
		return
	}

	//获取上下文的客户端信息
	cli, _ := conn.Context().(*Client)
	if cli == nil {
		conn.WriteError(errClientIsNil.Error())
		return
	}
	switch command {
	case "quit":
		_ = conn.Close()
	default:
		if res, err := cmdFunc(cli, cmd.Args[1:]); err != nil {
			if err == rose.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
		} else {
			conn.WriteAny(res)
		}
	}
}
