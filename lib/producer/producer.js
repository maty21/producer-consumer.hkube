const EventEmitter = require('events');
const Queue = require('bull');
const validate = require('djsv');
const uuidv4 = require('uuid/v4');
const schema = require('./schema');
const json = require('../helpers/json');
const redis = require('../helpers/redis');
const JobData = require('../entities/JobData')

class Producer extends EventEmitter {

    constructor(options) {
        super();
        this._setting = Object.assign({}, options.setting);
        const res = validate(schema.properties.setting, this._setting);
        if (!res.valid) {
            throw new Error(res.errors[0].stack);
        }
        this._jobMap = new Map();
        this._queues = new Map();
        redis.init(this._setting.redis);
        this._setting = Object.assign({}, this._setting, redis.client);
    }

    createJob(options) {
        return new Promise((resolve, reject) => {
            options = options || {};
            const res = validate(schema, options);
            if (!res.valid) {
                return reject(new Error(res.errors[0].stack));
            }
            let queue = this._queues.get(options.job.type);
            if (!queue) {
                queue = new Queue(options.job.type, this._setting);
                queue.on('global:waiting', (jobID, type) => {
                    const job = this._jobMap.get(jobID);
                    if (job) {
                        clearTimeout(job.timeout);
                        const jobData = this._createJobData(jobID, job.options);
                        this.emit('job-waiting', jobData);
                        if (job.options.resolveOnWaiting) {
                            job.resolve(jobData);
                        }
                    }
                }).on('global:active', (jobID, jobPromise) => {
                    const job = this._jobMap.get(jobID);
                    if (job) {
                        const jobData = this._createJobData(jobID, job.options);
                        this.emit('job-active', jobData);
                        if (job.options.resolveOnStart) {
                            job.resolve(jobData);
                        }
                    }
                }).on('global:failed', (jobID, err) => {
                    const job = this._jobMap.get(jobID);
                    if (job) {
                        clearTimeout(job.timeout);
                        const jobData = this._createJobData(jobID, job.options, err);
                        this.emit('job-failed', jobData);
                        this._jobMap.delete(job.id);
                        job.reject(err);
                    }
                }).on('global:completed', (jobID, result) => {
                    const job = this._jobMap.get(jobID);
                    if (job) {
                        const jobData = this._createJobData(jobID, job.options, null, json.tryParse(result));
                        this.emit('job-completed', jobData);
                        this._jobMap.delete(job.id);
                        if (job.options.resolveOnComplete) {
                            job.resolve(jobData);
                        }
                    }
                }).on('error', (error) => {
                    this.emit('job-error', error);
                });
                this._queues.set(options.job.type, queue);
            }
            let job, timer;
            const jobID = options.job.id || this.createJobID(options.job.type);
            options.queue.jobId = jobID;

            if (options.job.waitingTimeout > 0) {
                timer = setTimeout(() => {
                    if (job) {
                        job.discard();
                        job.remove();
                    }
                    this._jobMap.delete(jobID);
                    return reject(new Error(`job-waiting-timeout (id: ${jobID})`));
                }, options.job.waitingTimeout);
            }
            this._jobMap.set(jobID, {
                id: jobID,
                timeout: timer,
                resolve: resolve,
                reject: reject,
                options: options.job
            });
            queue.add(options.job.type, options.job.data, options.queue).then((result) => {
                job = result;
                if (!options.job.resolveOnStart && !options.job.resolveOnComplete) {
                    return resolve(jobID);
                }
            }).catch((error) => {
                return reject(error);
            });
        })
    }

    setJobsState(jobs) {
        jobs.forEach(j => {
            this._jobMap.set(j.key, {
                id: j.key,
                options: j.value
            });
        });
    }

    async getJob(options) {
        return await this._queue.getJob(options.jobID);
    }

    _createJobData(jobID, options, error, result) {
        return new JobData({
            id: jobID,
            options: options,
            prefix: this._setting.prefix,
            error: error,
            result: result
        });
    }

    createJobID(type) {
        return [type, uuidv4()].join(':');
    }
}

module.exports = Producer;

