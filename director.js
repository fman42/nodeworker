const Worker = require('./worker');

const Director = function ({
                               queueLimit, threads, keysQueueKey, valuesQueueKey, workLoopInterval,
                               dynamicThreadsOptions, connections, params, methods,
                               workerOptions, prioritizedQueue, detailLog, onWorkError, onWorkSuccess,
                               logger, perWorkerLimit, itemBeforePushToQueue, prepareQueueFunction
                           }) {

    if (!keysQueueKey) throw new Error('keysQueueKey is required');

    if (!queueLimit) queueLimit = 5000;
    if (!perWorkerLimit) perWorkerLimit = 1500;
    if (!threads) threads = 1;
    if (!workerOptions) workerOptions = {};
    if (!keysQueueKey) keysQueueKey = 'writer_keys';
    if (!valuesQueueKey) valuesQueueKey = keysQueueKey + '_values';
    if (!workLoopInterval) workLoopInterval = 1000;

    if (prepareQueueFunction) {
        if (typeof prepareQueueFunction !== 'function' || prepareQueueFunction.constructor.name !== "AsyncFunction") {
            throw new Error('prepareQueueFunction must be an async function');
        }
    }

    if (!prepareQueueFunction) {
        prepareQueueFunction = async (queue) => {
            return queue;
        }
    }


    this.prepareFunction = async function () {
        const prepared = await prepareQueueFunction(this.queue);
        this.queue = prepared ? prepared : this.queue;
    }

    if (!logger) {
        this.logger = {
            info() {
                console.log(...arguments);
            },
            error() {
                console.error(...arguments)
            }
        };
    } else this.logger = logger;

    if (!this.logger.info || !this.logger.error) throw new Error('Logger must have info and error methods');

    const useDynamicThreads = !!dynamicThreadsOptions;
    let workersLimit = 10;

    if (useDynamicThreads) {
        if (dynamicThreadsOptions.workersLimit) {
            workersLimit = dynamicThreadsOptions.workersLimit;
        }
    }


    this.queue = [];
    this.workers = {};

    this.workInterval = null;

    if (!connections.redis || !connections) throw new Error('Connection with name redis is required');

    this.connections = connections;
    this.detailLog = detailLog;

    this.createWorker = function (pool = false) {
        const id = this.countWorkers(pool) + 1;
        if (useDynamicThreads && id > workersLimit) return false;

        if (this.workers[id]) throw new Error('Trying to reassign existing worker with id - ' + id);

        worker = new Worker(this, {id, ...workerOptions});

        return worker;
    };


    this.run = function () {
        for (let i = 0; i < threads; i++) {
            const worker = this.createWorker();
            this.workers[worker.id] = worker;
        }

        this.startWork();
    };

    this.fillQueue = function () {
        return new Promise((res, rej) => {
            if (!this.connections.redis) return rej(new Error('Redis is not connected'));
            if (this.queue.length > 1) return res(true);
            const keysOnGet = async (err, keys) => {
                if (err !== null || (keys && typeof keys === 'object' && keys.length === 0)) {
                    return err ? rej(err) : res(true);
                }

                // this.queue = keys.splice(0, queueLimit);
                this.connections.redis.hmget(valuesQueueKey, keys, (err, values) => {
                    if (err !== null || (keys && typeof values === 'object' && values.length === 0)) {
                        return err ? rej(err) : res(true);
                    }
                    this.queue = values.reduce((acc, item) => {
                        if (typeof itemBeforePushToQueue === 'function') item = itemBeforePushToQueue(item);
                        if (item) acc.push(item);

                        return acc;
                    }, []);
                    return res(true);
                });

            };

            if (prioritizedQueue) this.connections.redis.zrange(keysQueueKey, 0, queueLimit, keysOnGet);
            else this.connections.redis.srandmember(keysQueueKey, queueLimit, keysOnGet);
        });
    };

    this.startWork = function () {
        if (!this.workInterval) this.workInterval = setInterval(() => this.workLoop(), workLoopInterval);
    };

    this.workLoop = async function () {
        try {
            await this.fillQueue()
            await this.prepareFunction();
            const workersPool = {...this.workers};
            const calcIPW = () => Math.ceil(this.queue.length / this.countWorkers(workersPool))
            let itemsPerWorker = calcIPW();

            if (useDynamicThreads) {
                while ((itemsPerWorker > perWorkerLimit) && this.countWorkers(workersPool) < workersLimit) {
                    const worker = this.createWorker(workersPool);
                    worker.isTemp = true;

                    if (worker) workersPool[worker.id] = worker;
                    else break;

                    itemsPerWorker = calcIPW();
                }
            }

            if (itemsPerWorker > perWorkerLimit) itemsPerWorker = perWorkerLimit;

            if (this.queue.length && this.detailLog) this.logger.info('QUEUE LENGTH ' + this.queue.length);

            for (let workerId in workersPool) {
                if (workersPool.hasOwnProperty(workerId)) {

                    const worker = workersPool[workerId];
                    if (!worker.queue.length) {
                        worker.fillQueue(this.queue.splice(0, itemsPerWorker));
                    }

                    if (!worker.isBusy && worker.queue.length) {
                        worker.work().then(data => {
                            if (typeof onWorkSuccess === 'function') onWorkSuccess(this, worker, data);
                        }).catch(err => {
                            if (this.detailLog) this.logger.error(err);
                            if (typeof onWorkError === 'function') onWorkError(this, worker, err);
                        });
                    }
                }
            }
        } catch (err) {
            this.logger.error(err);
            clearInterval(this.workInterval);
            this.startWork();
        }
    };

    this.removeFromRedis = function (key) {
        if (prioritizedQueue) this.connections.redis.zrem(keysQueueKey, key);
        else this.connections.redis.srem(keysQueueKey, key);

        this.connections.redis.hdel(valuesQueueKey, key);
        if (this.detailLog) this.logger.info('DELETING - ' + key);
    };

    this.countWorkers = (pool = false) => Object.keys(pool ? pool : this.workers).length;

    if (params) {
        for (let paramKey in params) {
            if (params.hasOwnProperty(paramKey)) {
                if (this.hasOwnProperty(paramKey)) throw new Error('Unavailable name for param ' + paramKey);
                this[paramKey] = params[paramKey];
            }
        }
    }

    if (methods) {
        for (let methodKey in methods) {
            if (methods.hasOwnProperty(methodKey)) {
                if (this.hasOwnProperty(methodKey)) throw new Error('Unavailable name for method ' + methodKey);
                this[methodKey] = methods[methodKey];
            }
        }
    }
};

module.exports = Director;