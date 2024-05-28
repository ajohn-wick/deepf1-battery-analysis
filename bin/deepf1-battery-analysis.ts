#!/usr/bin/env node
import 'source-map-support/register';
import { App } from 'aws-cdk-lib';
import { DeepF1BatteryAnalysisStack } from '../lib/deepf1-battery-analysis-stack';

const app = new App();
new DeepF1BatteryAnalysisStack(app, 'DeepF1BatteryAnalysisStack', {
    env: {
        account: process.env.AWS_ACCOUNT_ID,
        region: process.env.AWS_REGION
    }
});