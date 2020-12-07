import { expect } from 'chai';
import 'mocha';

import { logDriverMqtt } from '../src/index';

describe('template_error test', () => {
    it('should create instance', () => {
        let i = new logDriverMqtt({ mqttClientConfig: {}, logTopic: "" })
        expect(i).is.instanceof(logDriverMqtt);
        i.completeTransfer()
    })
})