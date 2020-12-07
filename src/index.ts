import { blankLogger, logDriverBase, tLogger, tLogLevel } from "tag-tree-logger"
import { connect as mqttConnect, IClientOptions, MqttClient } from "mqtt"
import { difference, union, intersection, reduce, find } from "lodash";

export interface tLogDriverMqttCfg {
    mqttClientConfig: IClientOptions,
    logTopic: string,
    logTopicIncludeTag?: boolean,
    mqttKeepTimerInterval_ms?: number,
    mqttKeepTimerDisable?: boolean
}

export class logDriverMqtt extends logDriverBase {
    readonly mqttClient: MqttClient
    readonly logger: tLogger
    readonly logTopic: string
    readonly logTopicIncludeTag: boolean
    readonly mqttKeepTimerInterval_ms: number
    readonly mqttKeepTimerDisable: boolean
    private mqttConnectionRetryTimer: NodeJS.Timeout | undefined
    mqttConnected: boolean = false
    constructor(readonly config: tLogDriverMqttCfg, readonly parentLogger: tLogger = blankLogger) {
        super()
        this.logger = parentLogger.logger(["logDriverMqtt"])
        this.logTopic = this.config.logTopic
        this.logTopicIncludeTag = this.config.logTopicIncludeTag ? this.config.logTopicIncludeTag : false
        this.mqttKeepTimerInterval_ms = this.config.mqttKeepTimerInterval_ms ? this.config.mqttKeepTimerInterval_ms : 1000 * 10
        this.mqttKeepTimerDisable = this.config.mqttKeepTimerDisable ? this.config.mqttKeepTimerDisable : false
        this.mqttClient = mqttConnect(this.config.mqttClientConfig);
        this.mqttClient.on("connect", () => {
            this.mqttConnected = true
            this.logger.info(`Mqtt connected`)
        })
        this.mqttClient.on('error', (error) => {
            this.mqttConnected = false;
            this.logger.error(`${error.message || error}`, error)
        })
        this.mqttClient.on("close", () => {
            this.mqttConnected = false;
            this.logger.warn(`MQTT client close`);
        })
        this.mqttClient.on("offline", () => {
            this.mqttConnected = false;
            this.logger.warn(`MQTT client offline`);
        })
        if (this.mqttKeepTimerDisable === false) {
            this.mqttConnectionRetryTimer = setInterval(() => {
                if (this.mqttConnected === false) {
                    this.logger.info(`try reconnect mqtt client`)
                    this.mqttClient.reconnect()
                }
            }, this.mqttKeepTimerInterval_ms)
        }
    }
    output(level: tLogLevel, tags: string[], msg: string, timestamp: Date, data: unknown): void {
        if (level >= tLogLevel.log) {
            if (!this.logFilter(tags)) {
                // log disabled
                return;
            }
        }
        const tagString = this.genTagString(tags)
        let packet = {
            message: msg,
            level: level,
            tag: tagString,
            tags: tags,
            timestamp: timestamp.toISOString(),
            data: data
        }
        let topic = this.logTopic;
        if (this.logTopicIncludeTag) {
            topic = `${topic}/${tagString}`
        }
        this.mqttClient.publish(topic, JSON.stringify(packet))
    }
    private genTagString(tags: string[]) {
        return tags.reduce((sum: string, tag: string, cnt: number) => {
            if (cnt === 0) {
                return sum + tag;
            } else {
                return sum + '/' + tag;
            }
        }, "")
    }
    private logEnabledTags: string[] = []
    private logFilter(tags: string[]): boolean {
        return (intersection(tags, this.logEnabledTags).length !== 0)
    }
    logEnable(tags: string[]) {
        this.logEnabledTags = union(this.logEnabledTags, tags);
    }
    logDisable(tags: string[]) {
        this.logEnabledTags = difference(this.logEnabledTags, tags);
    }
    completeTransfer(): Promise<void> | void {
        if (this.mqttConnectionRetryTimer) {
            clearInterval(this.mqttConnectionRetryTimer)
        }
        this.mqttConnectionRetryTimer = undefined
        return new Promise(resolve => {
            this.mqttClient.end(false, {}, () => { resolve() })
        })
    }
}