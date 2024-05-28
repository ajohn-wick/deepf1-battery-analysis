/***** CDK *****/
import { Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
/***** END CDK *****/

import { StorageService } from './storage-service';
import { MicroService } from './micro-service';
import { DataService } from './data-service';
import { OrchestratorService } from './orchestrator-service';
const randomstring = require('randomstring');

/***** ENVIRONMENT VARIABLES *****/
const AWS_ACCOUNT = process.env.AWS_ACCOUNT_ID || '';
const AWS_REGION = process.env.AWS_REGION || '';
const SERVICE_PREFIX = 'DeepF1BA';
const RESOURCE_PREFIX = 'deepf1-ba';
const RANDOM_STR = randomstring.generate(7).toLowerCase();
/***** END ENVIRONMENT VARIABLES *****/

export class DeepF1BatteryAnalysisStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);
    if (AWS_ACCOUNT !== '' && AWS_REGION !== '') {
      const storageService = new StorageService(this, SERVICE_PREFIX + 'StorageService', {
        appPrefix: 'StorageService',
        resourcePrefix: RESOURCE_PREFIX,
        awsAccount: AWS_ACCOUNT,
        awsRegion: AWS_REGION,
        randomSTR: RANDOM_STR
      });
      const dataService = new DataService(this, SERVICE_PREFIX + 'DataService', {
        appPrefix: 'DataService',
        resourcePrefix: RESOURCE_PREFIX,
        awsAccount: AWS_ACCOUNT,
        awsRegion: AWS_REGION,
        randomSTR: RANDOM_STR,
        s3RAWBucket: storageService.getS3RAWBucket(),
        s3STDBucket: storageService.getS3STDBucket()
      });
      const microService = new MicroService(this, SERVICE_PREFIX + 'MicroService', {
        appPrefix: 'MicroService',
        resourcePrefix: RESOURCE_PREFIX,
        awsAccount: AWS_ACCOUNT,
        awsRegion: AWS_REGION,
        randomSTR: RANDOM_STR,
        s3RAWBucket: storageService.getS3RAWBucket(),
        glueSTDCrawler: dataService.getGlueSTDCrawler()
      });
      const orchestratorService = new OrchestratorService(this, SERVICE_PREFIX + 'OrchestratorService', {
        appPrefix: SERVICE_PREFIX,
        resourcePrefix: RESOURCE_PREFIX,
        awsAccount: AWS_ACCOUNT,
        awsRegion: AWS_REGION,
        randomSTR: RANDOM_STR,
        s3RAWBucket: storageService.getS3RAWBucket(),
        s3STDBucket: storageService.getS3STDBucket(),
        lambdaIngestRAW: microService.getLambdaIngestRAWFunction(),
        lambdaListRAW: microService.getLambdaListRAWFunction(),
        glueSTDJob: dataService.getGlueSTDJob(),
        lambdaRunSTDCrawler: microService.getLambdaRunSTDCrawlerFunction(),
        lambdaCheckSTDCrawler: microService.getLambdaCheckSTDCrawlerFunction()
      });
    }
  }
}