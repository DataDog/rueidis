package conn

import (
	"bufio"
	"errors"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rueian/rueidis/internal/cache"
	"github.com/rueian/rueidis/internal/cmds"
	"github.com/rueian/rueidis/internal/proto"
	"github.com/rueian/rueidis/internal/queue"
)

const DefaultCacheBytes = 128 * (1 << 20)

var (
	ErrConnClosing = errors.New("connection is closing")
)

type wire struct {
	waits int32
	state int32

	conn  net.Conn
	queue queue.Queue
	cache cache.Cache
	error atomic.Value

	r *bufio.Reader
	w *bufio.Writer

	info proto.Message
}

type Option struct {
	CacheSize  int
	SelectDB   int
	Username   string
	Password   string
	ClientName string
}

func newWire(conn net.Conn, option Option) (*wire, error) {
	if option.CacheSize <= 0 {
		option.CacheSize = DefaultCacheBytes
	}

	c := &wire{
		conn:  conn,
		queue: queue.NewRing(),
		cache: cache.NewLRU(option.CacheSize),
		r:     bufio.NewReader(conn),
		w:     bufio.NewWriter(conn),
	}
	go c.reading()

	helloCmd := []string{"HELLO", "3"}
	if option.Username != "" {
		helloCmd = append(helloCmd, "AUTH", option.Username, option.Password)
	}
	if option.ClientName != "" {
		helloCmd = append(helloCmd, "SETNAME", option.ClientName)
	}

	init := [][]string{helloCmd, {"CLIENT", "TRACKING", "ON", "OPTIN"}}
	if option.SelectDB != 0 {
		init = append(init, []string{"SELECT", strconv.Itoa(option.SelectDB)})
	}

	resp := c.DoMulti(cmds.NewMultiCompleted(init)...)
	for _, r := range resp {
		if r.Err != nil {
			return nil, r.Err
		}
	}

	c.info = resp[0].Val

	return c, nil
}

func (c *wire) reading() {
	wg := sync.WaitGroup{}
	wg.Add(2)
	exit := func() {
		_ = c.conn.Close() // force both read & write goroutine to exit
		wg.Done()
	}
	go func() { // write goroutine
		defer exit()

		var err error
		for atomic.LoadInt32(&c.state) != 2 {
			if one, multi := c.queue.NextCmd(); !one.IsEmpty() {
				err = proto.WriteCmd(c.w, one.Commands())
			} else if multi != nil {
				for _, cmd := range multi {
					err = proto.WriteCmd(c.w, cmd.Commands())
				}
			} else {
				err = c.w.Flush()
				runtime.Gosched()
			}
			if err != nil {
				c.error.CompareAndSwap(nil, err)
				return
			}
		}
	}()
	go func() { // read goroutine
		defer exit()
		for {
			msg, err := proto.ReadNextMessage(c.r)
			if err != nil {
				c.error.CompareAndSwap(nil, err)
				return
			}
			if msg.Type == '>' {
				c.handlePush(msg.Values)
				continue
			}
			_, multi, ch := c.queue.NextResultCh()
			if ch == nil {
				panic("receive unexpected out of band message")
			}
			ch <- proto.Result{Val: msg, Err: err}

			if multi == nil {
				continue
			}

			// read the rest cmd responses
			for i := 1; i < len(multi); {
				msg, err = proto.ReadNextMessage(c.r)
				if err != nil {
					c.error.CompareAndSwap(nil, err)
					for ; i < len(multi); i++ {
						ch <- proto.Result{Val: msg, Err: err}
					}
					return
				}
				if msg.Type == '>' {
					c.handlePush(msg.Values)
					continue
				}
				// in current implementation, the cache opt-in will only be in the first cmd in batch
				// TODO: handle opt-in cache for MULTI, LUA commands
				if msg.Type != '-' && msg.Type != '!' && multi[0].IsOptIn() {
					cacheable := cmds.Cacheable(multi[i])
					ck, cc := cacheable.CacheKey()
					c.cache.Update(ck, cc, msg)
				}
				ch <- proto.Result{Val: msg, Err: err}
				i++
			}
		}
	}()
	wg.Wait()
	atomic.CompareAndSwapInt32(&c.state, 0, 1)

	// clean up write queue and read queue
	for atomic.LoadInt32(&c.waits) != 0 {
		c.queue.NextCmd()
		if one, multi, ch := c.queue.NextResultCh(); !one.IsEmpty() {
			ch <- proto.Result{Err: c.error.Load().(error)}
		} else if multi != nil {
			for i := 0; i < len(multi); i++ {
				ch <- proto.Result{Err: c.error.Load().(error)}
			}
		} else {
			runtime.Gosched()
		}
	}

	atomic.CompareAndSwapInt32(&c.state, 1, 2)
}

func (c *wire) handlePush(values []proto.Message) {
	// TODO: handle other push data
	// tracking-redir-broken
	// message
	// pmessage
	// subscribe
	// unsubscribe
	// psubscribe
	// punsubscribe
	// server-cpu-usage
	if len(values) < 2 {
		return
	}
	if values[0].String == "invalidate" {
		c.cache.Delete(values[1].Values)
	}
}

func (c *wire) Info() proto.Message {
	return c.info
}

func (c *wire) Do(cmd cmds.Completed) (resp proto.Result) {
	atomic.AddInt32(&c.waits, 1)
	if atomic.LoadInt32(&c.state) == 0 {
		resp = <-c.queue.PutOne(cmd)
	} else {
		resp.Err = c.error.Load().(error)
	}
	atomic.AddInt32(&c.waits, -1)
	return resp
}

func (c *wire) DoMulti(multi ...cmds.Completed) []proto.Result {
	resp := make([]proto.Result, len(multi))
	atomic.AddInt32(&c.waits, 1)
	if atomic.LoadInt32(&c.state) == 0 {
		ch := c.queue.PutMulti(multi)
		for i := range resp {
			resp[i] = <-ch
		}
	} else {
		err := c.error.Load().(error)
		for i := 0; i < len(resp); i++ {
			resp[i].Err = err
		}
	}
	atomic.AddInt32(&c.waits, -1)
	return resp
}

func (c *wire) DoCache(cmd cmds.Cacheable, ttl time.Duration) proto.Result {
retry:
	ck, cc := cmd.CacheKey()
	if v, ch := c.cache.GetOrPrepare(ck, cc, ttl); v.Type != 0 {
		return proto.Result{Val: v}
	} else if ch != nil {
		<-ch
		goto retry
	}
	return c.DoMulti(cmds.OptInCmd, cmds.Completed(cmd))[1]
}

func (c *wire) Error() error {
	if err, ok := c.error.Load().(error); ok {
		return err
	}
	return nil
}

func (c *wire) Close() {
	swapped := c.error.CompareAndSwap(nil, ErrConnClosing)
	atomic.CompareAndSwapInt32(&c.state, 0, 1)
	for atomic.LoadInt32(&c.waits) != 0 {
		runtime.Gosched()
	}
	if swapped {
		<-c.queue.PutOne(cmds.QuitCmd)
	}
	atomic.CompareAndSwapInt32(&c.state, 1, 2)
}