const { expect } = require('chai');
const Redis = require('ioredis');
const { Producer, Consumer } = require('../index');
const Queue = require('bull');

const redisHost = process.env.REDIS_CLUSTER_SERVICE_HOST || '127.0.0.1';
const redisPort = process.env.REDIS_CLUSTER_SERVICE_PORT || "6379";
const useCluster = process.env.REDIS_CLUSTER_SERVICE_HOST ? true : false;
const redisConfig = { host: redisHost, port: redisPort, cluster: useCluster };

const { tracer } = require('@hkube/metrics')
const { InMemoryReporter, ConstSampler, RemoteReporter } = require('jaeger-client');

const globalOptions = {
    job: {
        type: 'test-job-global',
        data: { action: 'bla' },
        waitingTimeout: 5000
    },
    queue: {
        priority: 1,
        delay: 1000,
        timeout: 5000,
        attempts: 3,
        removeOnComplete: true,
        removeOnFail: false
    },
    setting: {
        prefix: 'sf-jobs',
        redis: {
            host: '127.0.0.1',
            port: "6379",
            cluster: true,
            sentinel: false
        }
    }
}

describe('Tracing', () => {
    beforeEach((done) => {
        tracer._spanStack = [];
        if (tracer._tracer) {
            tracer._tracer.close(() => {
                tracer._tracer = null;
                done();
            });
        }
        else {
            done();
        }
    });
    it('should work without tracing', () => {
        let job = null;
        const res = { success: true };
        const options = {
            job: {
                type: 'tracing-test',
                data: { action: 'bla' },
            }
        }
        const producer = new Producer(options);
        producer.on('job-completed', (data) => {
            expect(data.jobID).to.be.a('string');
            expect(data.result).to.deep.equal(res);
            done();
        });
        const consumer = new Consumer(options);
        consumer.on('job', (job) => {
            job.done(null, res);
        });
        consumer.register(options);
        producer.createJob(options);
    });

    it('should work with tracing', async () => {
        await tracer.init({
            tracerConfig: {
                serviceName: 'test',
            },
            tracerOptions: {
                reporter: new InMemoryReporter()
            }

        });
        let job = null;
        const res = { success: true };
        const options = {
            job: {
                type: 'tracing-test-2',
                data: { action: 'bla' },
            },
            tracing: {

            },
            setting: {
                tracer
            }
        }
        const producer = new Producer(options);
        producer.on('job-completed', (data) => {
            expect(data.jobID).to.be.a('string');
            expect(data.result).to.deep.equal(res);
            expect(job.data.spanId).to.not.be.empty
            done();
        });
        const consumer = new Consumer(options);
        consumer.on('job', (job) => {
            expect(job.data.spanId).to.not.be.empty
            job.done(null, res);
        });
        consumer.register(options);
        producer.createJob(options);
    });
});