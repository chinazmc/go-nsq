package nsq

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type producerConn interface {
	String() string
	SetLogger(logger, LogLevel, string)
	SetLoggerLevel(LogLevel)
	SetLoggerForLevel(logger, LogLevel, string)
	Connect() (*IdentifyResponse, error)
	Close() error
	WriteCommand(*Command) error
}

// Producer is a high-level type to publish to NSQ.
//
// A Producer instance is 1:1 with a destination `nsqd`
// and will lazily connect to that instance (and re-connect)
// when Publish commands are executed.
type Producer struct {
	id     int64        // 用于打印日志时标示实例。由instCount全局变量控制，从0开始，每创建一个Producer或Consumer时+1
	addr   string       // 连接的Nsqd的地址
	conn   producerConn // 连接实例
	config Config       // 配置参数

	logger   []logger
	logLvl   LogLevel
	logGuard sync.RWMutex

	responseChan chan []byte // conn收到Nsqd生产成功的响应后，通过此chan告知router()，router再通知到生产消息的线程
	errorChan    chan []byte // conn收到Nsqd错误信息的响应后，通过此chan告知router()，router再通知到生产消息的线程
	closeChan    chan int    // conn断开时通过此chan告知router()结束

	transactionChan chan *ProducerTransaction // 生产过程将消息推送到这个chan，再异步接收成功的结果
	transactions    []*ProducerTransaction    // 一个先入先出队列，用于router协程处理消息写入结果
	state           int32                     // 连接状态，初始状态/连接断开/已连接

	concurrentProducers int32 // 统计正在等待发往transactionChan的消息数，Producer在退出前会将这些消息置为ErrNotConnected
	stopFlag            int32
	exitChan            chan int
	wg                  sync.WaitGroup // 用于等待router()协程退出
	guard               sync.Mutex     // Producer全局锁
}

// ProducerTransaction is returned by the async publish methods
// to retrieve metadata about the command after the
// response is received.
type ProducerTransaction struct {
	cmd      *Command
	doneChan chan *ProducerTransaction
	Error    error         // the error (or nil) of the publish command
	Args     []interface{} // the slice of variadic arguments passed to PublishAsync or MultiPublishAsync
}

func (t *ProducerTransaction) finish() {
	if t.doneChan != nil {
		t.doneChan <- t
	}
}

// NewProducer returns an instance of Producer for the specified address
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewProducer the values are no longer mutable (they are copied).
func NewProducer(addr string, config *Config) (*Producer, error) {
	//检查配置文件，是否初始化，验证是否成功
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	//实例化Producer
	p := &Producer{
		//id 自增1
		id: atomic.AddInt64(&instCount, 1),

		addr:   addr,
		config: *config, //配置文件

		logger: make([]logger, int(LogLevelMax+1)),
		logLvl: LogLevelInfo,

		transactionChan: make(chan *ProducerTransaction),
		exitChan:        make(chan int),
		responseChan:    make(chan []byte),
		errorChan:       make(chan []byte),
	}

	// Set default logger for all log levels
	l := log.New(os.Stderr, "", log.Flags())
	for index, _ := range p.logger {
		p.logger[index] = l
	}
	return p, nil
}

// Ping causes the Producer to connect to it's configured nsqd (if not already
// connected) and send a `Nop` command, returning any error that might occur.
//
// This method can be used to verify that a newly-created Producer instance is
// configured correctly, rather than relying on the lazy "connect on Publish"
// behavior of a Producer.
// Ping方法一般用于刚创建的Producer实例。自动connect()方法创建连接，并发送一条Nop指令，以确认连接是否正常
func (w *Producer) Ping() error {
	if atomic.LoadInt32(&w.state) != StateConnected {
		err := w.connect()
		if err != nil {
			return err
		}
	}

	return w.conn.WriteCommand(Nop())
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (w *Producer) SetLogger(l logger, lvl LogLevel) {
	w.logGuard.Lock()
	defer w.logGuard.Unlock()

	for level := range w.logger {
		w.logger[level] = l
	}
	w.logLvl = lvl
}

// SetLoggerForLevel assigns the same logger for specified `level`.
func (w *Producer) SetLoggerForLevel(l logger, lvl LogLevel) {
	w.logGuard.Lock()
	defer w.logGuard.Unlock()

	w.logger[lvl] = l
}

// SetLoggerLevel sets the package logging level.
func (w *Producer) SetLoggerLevel(lvl LogLevel) {
	w.logGuard.Lock()
	defer w.logGuard.Unlock()

	w.logLvl = lvl
}

func (w *Producer) getLogger(lvl LogLevel) (logger, LogLevel) {
	w.logGuard.RLock()
	defer w.logGuard.RUnlock()

	return w.logger[lvl], w.logLvl
}

func (w *Producer) getLogLevel() LogLevel {
	w.logGuard.RLock()
	defer w.logGuard.RUnlock()

	return w.logLvl
}

// String returns the address of the Producer
func (w *Producer) String() string {
	return w.addr
}

// Stop initiates a graceful stop of the Producer (permanent)
//
// NOTE: this blocks until completion
// Stop()方法用于优雅退出当前Producer。
// 正在等待发送的消息将被置为ErrNotConnected或ErrStopped
func (w *Producer) Stop() {
	w.guard.Lock()
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		w.guard.Unlock()
		return
	}
	w.log(LogLevelInfo, "(%s) stopping", w.addr)
	close(w.exitChan)
	w.close()
	w.guard.Unlock()
	w.wg.Wait()
}

// PublishAsync publishes a message body to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
//非阻塞发布1条消息。相比Publish()，多了一个额外的doneChan参数，通过此chan来异步接收发布结果。
func (w *Producer) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	return w.sendCommandAsync(Publish(topic, body), doneChan, args)
}

// MultiPublishAsync publishes a slice of message bodies to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
//非阻塞发布多条消息。通过doneChan来异步接收发布结果。
func (w *Producer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *ProducerTransaction,
	args ...interface{}) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return w.sendCommandAsync(cmd, doneChan, args)
}

// Publish synchronously publishes a message body to the specified topic, returning
// an error if publish failed
//阻塞发布1条消息。底层调用"PUB"指令
func (w *Producer) Publish(topic string, body []byte) error {
	return w.sendCommand(Publish(topic, body))
}

// MultiPublish synchronously publishes a slice of message bodies to the specified topic, returning
// an error if publish failed
//阻塞发布多条消息。底层调用"MPUB"指令
func (w *Producer) MultiPublish(topic string, body [][]byte) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return w.sendCommand(cmd)
}

// DeferredPublish synchronously publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires, returning
// an error if publish failed
//阻塞发布1条带延时的消息。相比Publish()，多了一个delay参数来指定延时多久才推送给消费者。底层调用"DPUB"指令
func (w *Producer) DeferredPublish(topic string, delay time.Duration, body []byte) error {
	return w.sendCommand(DeferredPublish(topic, delay, body))
}

// DeferredPublishAsync publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
//非阻塞发布1条带延时的消息。通过doneChan来异步接收发布结果。
func (w *Producer) DeferredPublishAsync(topic string, delay time.Duration, body []byte,
	doneChan chan *ProducerTransaction, args ...interface{}) error {
	return w.sendCommandAsync(DeferredPublish(topic, delay, body), doneChan, args)
}

//sendCommand()方法创建一个临时的doneChan来接收发布结果。
func (w *Producer) sendCommand(cmd *Command) error {
	//提前设置了一个接受返回参数的Chan, 这里有伏笔，埋伏它一手
	doneChan := make(chan *ProducerTransaction)
	//调用了sendCommandAsync 并且把doneChan 传进去了
	err := w.sendCommandAsync(cmd, doneChan, nil)
	if err != nil {
		close(doneChan)
		return err
	}
	//上面函数结束后，在这里苦苦的等待 doneChan的返回值，所以我们可以大胆的推测 sendCommandAsync 方法并不返回真实的值
	t := <-doneChan
	return t.Error
}

/**
sendCommandAsync()负责将消息写入Producer.transactionChan。
下一章节的router协程负责接收并将消息发往Nsqd，并将发布结果通过doneChan返回。如果连接尚未创建，这里会自动重建连接。
*/
func (w *Producer) sendCommandAsync(cmd *Command, doneChan chan *ProducerTransaction,
	args []interface{}) error {
	// keep track of how many outstanding producers we're dealing with
	// in order to later ensure that we clean them all up...
	atomic.AddInt32(&w.concurrentProducers, 1)
	defer atomic.AddInt32(&w.concurrentProducers, -1)

	if atomic.LoadInt32(&w.state) != StateConnected {
		err := w.connect() // 未连接时自动重建连接
		if err != nil {
			return err
		}
	}

	t := &ProducerTransaction{
		cmd:      cmd,
		doneChan: doneChan,
		Args:     args,
	}

	select {
	case w.transactionChan <- t: // 将消息发送给router协程
	case <-w.exitChan:
		return ErrStopped
	}

	return nil
}

// 创建连接，修改连接状态，启动router()协程
func (w *Producer) connect() error {
	w.guard.Lock()
	defer w.guard.Unlock()

	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return ErrStopped
	}

	state := atomic.LoadInt32(&w.state)
	switch {
	case state == StateConnected:
		return nil
	case state != StateInit:
		return ErrNotConnected
	}

	w.log(LogLevelInfo, "(%s) connecting to nsqd", w.addr)

	w.conn = NewConn(w.addr, &w.config, &producerConnDelegate{w})
	w.conn.SetLoggerLevel(w.getLogLevel())
	format := fmt.Sprintf("%3d (%%s)", w.id)
	for index := range w.logger {
		w.conn.SetLoggerForLevel(w.logger[index], LogLevel(index), format)
	}

	_, err := w.conn.Connect()
	if err != nil {
		w.conn.Close()
		w.log(LogLevelError, "(%s) error connecting to nsqd - %s", w.addr, err)
		return err
	}
	atomic.StoreInt32(&w.state, StateConnected)
	w.closeChan = make(chan int)
	w.wg.Add(1)
	go w.router()

	return nil
}

func (w *Producer) close() {
	if !atomic.CompareAndSwapInt32(&w.state, StateConnected, StateDisconnected) {
		return
	}
	w.conn.Close()
	go func() {
		// we need to handle this in a goroutine so we don't
		// block the caller from making progress
		w.wg.Wait()
		atomic.StoreInt32(&w.state, StateInit)
	}()
}

/**
Producer通过起一个router协程异步发送和接收消息响应的方式来实现Producer并发写入的问题。无论用户有多少个线程在生产消息，
最终都得调用sendCommandAsync()方法将消息写入一个chan，并由router单协程处理，这就避免了并发冲突。
router协程在上文的Producer.connect()方法被启动。
Router()方法起了个for循环持续监听几个chan，我们重点关注：

transactionChan：所有消息最终均通过sendCommandAsync()方法写入这个chan。router协程负责将从transactionChan收到的消息，
写入Producer.conn，并最终发送到Nsqd。
responseChan：Nsqd每正确接收到一个消息，会响应一个确认帧回来。Producer.conn则通过此chan来告知router消息写入成功。
errorChan：同responseChan，收到错误信息时，通过此chan告知router。
无论是写入成功还是有错误，router协程均调用popTransaction()方法来处理。这个方法有个细节，Nsqd并没有告知写入成功或失败的消息是哪条，
Producer又是怎么知道的呢？原理是底层使用的TCP通讯，同学们可以回想下TCP的特点，TCP是有序的。写入消息的只有router一个协程，
所以消息是按顺序写入的，恰恰Nsqd端也是单线程处理同一个生产者。所以router收到的响应，必然是针对transactions队列中第1条消息的
（这是一个用切片实现的先入先出队列，router会在写入conn的同时将消息写入这个队列）。

收到Nsqd的响应后，router将结束写入ProducerTransaction.doneChan，用于通知消息的写入协程。
*/
func (w *Producer) router() {
	for {
		select {
		case t := <-w.transactionChan: // 这是待发布的消息
			w.transactions = append(w.transactions, t) // 先入先出队列，用于处理消息发布结果
			err := w.conn.WriteCommand(t.cmd)          // 发布消息
			if err != nil {
				w.log(LogLevelError, "(%s) sending command - %s", w.conn.String(), err)
				w.close()
			}
		case data := <-w.responseChan: // 发布成功的响应
			w.popTransaction(FrameTypeResponse, data) // 处理发布结果，将结果写入doneChan
		case data := <-w.errorChan: // 发布失败的响应
			w.popTransaction(FrameTypeError, data) // 处理发布结果，将结果写入doneChan
		case <-w.closeChan:
			goto exit
		case <-w.exitChan:
			goto exit
		}
	}

exit:
	w.transactionCleanup()
	w.wg.Done()
	w.log(LogLevelInfo, "(%s) exiting router", w.conn.String())
}

func (w *Producer) popTransaction(frameType int32, data []byte) {
	t := w.transactions[0]
	w.transactions = w.transactions[1:] // 发布成功或失败的消息，出队
	if frameType == FrameTypeError {
		t.Error = ErrProtocol{string(data)} // 发布失败的错误信息
	}
	t.finish() // 通知到doneChan
}

func (w *Producer) transactionCleanup() {
	// clean up transactions we can easily account for
	for _, t := range w.transactions {
		t.Error = ErrNotConnected
		t.finish()
	}
	w.transactions = w.transactions[:0]

	// spin and free up any writes that might have raced
	// with the cleanup process (blocked on writing
	// to transactionChan)
	for {
		select {
		case t := <-w.transactionChan:
			t.Error = ErrNotConnected
			t.finish()
		default:
			// keep spinning until there are 0 concurrent producers
			if atomic.LoadInt32(&w.concurrentProducers) == 0 {
				return
			}
			// give the runtime a chance to schedule other racing goroutines
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (w *Producer) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := w.getLogger(lvl)

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %3d %s", lvl, w.id, fmt.Sprintf(line, args...)))
}

func (w *Producer) onConnResponse(c *Conn, data []byte) { w.responseChan <- data }
func (w *Producer) onConnError(c *Conn, data []byte)    { w.errorChan <- data }
func (w *Producer) onConnHeartbeat(c *Conn)             {}
func (w *Producer) onConnIOError(c *Conn, err error)    { w.close() }
func (w *Producer) onConnClose(c *Conn) {
	w.guard.Lock()
	defer w.guard.Unlock()
	close(w.closeChan)
}
