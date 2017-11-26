const EventEmitter = require('events');
const Queue = require('bull');
const validate = require('djsv');
const schema = require('./schema');
const redis = require('../helpers/redis');

class Consumer extends EventEmitter {

    constructor(options) {
        super();
        const result = validate(schema.properties.setting, options.setting);
        if (!result.valid) {
            throw new Error(result.error);
        }
        this._setting = result.instance;
        this._queues = new Map();
        redis.init(this._setting.redis);
        this._setting = Object.assign({}, this._setting, redis.client);
    }

    register(option) {
        const result = validate(schema, option);
        if (!result.valid) {
            throw new Error(result.error);
        }
        const options = result.instance;
        let queue = this._queues.get(options.job.type);
        if (!queue) {
            queue = new Queue(options.job.type, this._setting);
            queue.process(options.job.type, (job, done) => {
                const res = {
                    id: job.id,
                    data: job.data,
                    type: job.name,
                    prefix: job.queue.keyPrefix,
                    done: done
                }
                this.emit('job', res);
            });
            this._queues.set(options.job.type, queue);
        }
    }

    pause(options) {
        let queue = this._queues.get(options.type);
        if (!queue) {
            throw new Error(`unable to find handler for ${options.type}`);
        }
        return queue.pause(true);
    }

    resume(options) {
        let queue = this._queues.get(options.type);
        if (!queue) {
            throw new Error(`unable to find handler for ${options.type}`);
        }
        return queue.resume(true);
    }
}

module.exports = Consumer;
