const Redis = require('ioredis');
const redis = new Redis({host: '0.0.0.0', port: 6380, db: 0});
const Director = require('./director');

const begin = new Date().getTime();
let end;

const keysQueueKey = 'test_writer';

const generateQueue = (count) => {
    for (let i = 0; i < count; i++) {

        const key = 'test_' + i;
        const item = JSON.stringify({action: i < count / 2, check: i, key});
        redis.sadd(keysQueueKey, key);
        redis.hset(keysQueueKey + '_values', key, item);
    }
};


const director = new Director({
    queueLimit: 100000,
    perWorkerLimit: 1000,
    keysQueueKey,
    detailLog: false,
    dynamicThreadsOptions: {
        workersLimit: 200,
    },

    async prepareQueueFunction(queue) {
        console.log('PREPARE FUNCTION WORKS GREAT');
    },

    itemBeforePushToQueue(item) {
        return JSON.parse(item);
    },

    params: {test: 'TEST DIRECTOR PARAM'},
    methods: {
        testMethod() {
            console.log('CALLED TEST DIRECTOR METHOD', this.test);
        },
        countGood(items) {
            return items.filter(item => !!item).length;
        },
        countBad(items) {
            this.testMethod();
            return items.filter(item => !item).length;
        }
    },
    threads: 1,
    onWorkSuccess: (dir, wrk, result) => {
        console.log(`WORKER ${wrk.id} REPORTS HE SUCCEED ${dir.countGood(result)} ITEMS & FAILED ${dir.countBad(result)}`);
        if (!dir.queue.length) end = new Date().getTime();
        console.log('IT WAS ' + ((end - begin) / 1000) + ' SEC');
    },
    onWorkError: (dir, wrk, err) => {
        console.error(err);
    },
    connections: {
        redis
    },
    workerOptions: {
        async workFunction(worker, item) {
            return new Promise((resolve, reject) => {
                if (item.action) {
                    worker.director.removeFromRedis(item.key);
                    resolve({message: 'WORK RESOLVED key - ' + item.key, key: item.key})
                } else reject(new Error('ACTION IS FALSE'));
            })
        },

        onWorkError(worker, item, err) {
            // worker.runTest('worker on err');
            // console.log('WORKER ' + worker.id + ' REPORTS ERROR ');
            worker.director.removeFromRedis(item.key);
        },
        params: {
            test: 'TEST WORKER PARAM'
        },
        methods: {
            runTest(from) {
                console.log('(worker) RUN TEST FROM ' + from, this.test, this.director.test);
            }
        }
    }
});


// setInterval(() => {
generateQueue(100000);
// }, 30);

director.run();

