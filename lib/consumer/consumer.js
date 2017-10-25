const EventEmitter = require('events');
const Queue = require('bull');
const validate = require('djsv');
const schema = require('./schema');
const redis = require('../helpers/redis');

class Consumer extends EventEmitter {

    constructor(options) {
        super();
        this._setting = Object.assign({}, options.setting);
        const res = validate(schema.properties.setting, this._setting);
        if (!res.valid) {
            throw new Error(res.errors[0].stack);
        }
        this._queues = new Map();
        redis.init(this._setting.redis);
        this._setting = Object.assign({}, this._setting, redis.client);
    }

    register(options) {
        options = options || {};
        const res = validate(schema, options);
        if (!res.valid) {
            throw new Error(res.errors[0].stack);
        }
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
