/***** CDK *****/
import { Construct } from 'constructs';
import { Duration } from 'aws-cdk-lib';
/***** END CDK *****/

import { Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Function } from 'aws-cdk-lib/aws-lambda';
import { Job } from '@aws-cdk/aws-glue-alpha';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import { LambdaInvoke, GlueStartJobRun } from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Rule, Schedule, RuleTargetInput } from 'aws-cdk-lib/aws-events';
import { SfnStateMachine } from 'aws-cdk-lib/aws-events-targets';
const env = require('../resources/config/env.json');

export interface OrchestratorServiceProps {
  appPrefix: string;
  resourcePrefix: string;
  awsAccount: string;
  awsRegion: string;
  randomSTR: string;
  s3RAWBucket: Bucket;
  s3STDBucket: Bucket;
  lambdaIngestRAW: Function;
  lambdaListRAW: Function;
  glueSTDJob: Job;
  lambdaRunSTDCrawler: Function;
  lambdaCheckSTDCrawler: Function;
}

export class OrchestratorService extends Construct {

  constructor(scope: Construct, id: string, props: OrchestratorServiceProps) {
    super(scope, id);

    const sfIngestRAWLambdaMap = new sfn.Map(this, props.appPrefix + 'IngestRAWMAPStage', {
      itemsPath: sfn.JsonPath.stringAt('$.BATTERY_DATASETS'),
      resultPath: sfn.JsonPath.DISCARD,
      outputPath: sfn.JsonPath.DISCARD,
      maxConcurrency: 10
    });
    sfIngestRAWLambdaMap.iterator(new LambdaInvoke(this, props.appPrefix + 'IngestRAWStage', {
      lambdaFunction: props.lambdaIngestRAW,
      retryOnServiceExceptions: true
    }));

    const sfListRAWLambda = new LambdaInvoke(this, props.appPrefix + 'ListRAWStage', {
      lambdaFunction: props.lambdaListRAW,
      inputPath: sfn.JsonPath.DISCARD,
      outputPath: '$.Payload',
      retryOnServiceExceptions: true
    });

    const sfSTDGlueJobMap = new sfn.Map(this, props.appPrefix + 'STDMAPStage', {
      itemsPath: sfn.JsonPath.stringAt('$.RAW_BATTERY_FILES'),
      resultPath: sfn.JsonPath.DISCARD,
      outputPath: sfn.JsonPath.DISCARD,
      maxConcurrency: 2
    });
    sfSTDGlueJobMap.iterator(new GlueStartJobRun(this, props.appPrefix + 'STDStage', {
      glueJobName: props.glueSTDJob.jobName,
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      arguments: sfn.TaskInput.fromObject({
        "--S3_RAW_BUCKET_NAME": props.s3RAWBucket.bucketName,
        "--S3_STD_BUCKET_NAME": props.s3STDBucket.bucketName,
        "--BATTERY_FILE_KEY.$": "$.BATTERY_FILE_KEY"
      })
    }));

    const sfRunCrawlerLambda = new LambdaInvoke(this, props.appPrefix + 'RunSTDCrawlerStage', {
      lambdaFunction: props.lambdaRunSTDCrawler,
      retryOnServiceExceptions: true
    });
    const sfCheckCrawlerLambda = new LambdaInvoke(this, props.appPrefix + 'CheckSTDCrawlerStage', {
      lambdaFunction: props.lambdaCheckSTDCrawler,
      outputPath: '$.Payload',
      retryOnServiceExceptions: true
    });
    const sfWaitForGlueCrawler = new sfn.Wait(this, props.appPrefix + 'WaitForGlueSTDCrawlerStage', {
      time: sfn.WaitTime.duration(Duration.seconds(30))
    });

    const batteryPipelineDefinition =
    sfIngestRAWLambdaMap
      .next(sfListRAWLambda)
      .next(sfSTDGlueJobMap)
      .next(sfRunCrawlerLambda)
      .next(sfWaitForGlueCrawler)
      .next(sfCheckCrawlerLambda)
      .next(
        new sfn.Choice(this, props.appPrefix + 'CheckSTDCrawlerChoiceStage')
          .when(sfn.Condition.stringEquals('$.CRAWLER_STATE', 'READY'), new sfn.Succeed(this, props.appPrefix + 'EndStage'))
          .otherwise(sfWaitForGlueCrawler)
      );

    const cwSFLogGroup = new LogGroup(this, props.appPrefix + 'CWSFLogGroup', {
      logGroupName: '/aws/stepfunctions/battery-pipeline-' + props.resourcePrefix + '-' + props.randomSTR,
      retention: RetentionDays.TWO_WEEKS
    });

    const iamSFCWEventRole = new Role(this, props.appPrefix + 'IAMSFCWEventRole', {
      roleName: props.resourcePrefix + '-states-battery-pipeline',
      assumedBy: new ServicePrincipal('states.amazonaws.com')
    });

    const batteryPipeline = new sfn.StateMachine(this, props.appPrefix + 'BatteryPipelineSF', {
      stateMachineName: 'battery-pipeline-' + props.resourcePrefix + '-' + props.randomSTR,
      definition: batteryPipelineDefinition,
      stateMachineType: sfn.StateMachineType.STANDARD,
      timeout: Duration.seconds(900),
      role: iamSFCWEventRole,
      logs: {
        destination: cwSFLogGroup,
        level: sfn.LogLevel.ALL,
      },
      tracingEnabled: true
    });

    const triggerBatteryPipeline = new Rule(this, props.appPrefix + 'BatteryPipelineCWEvent', {
        enabled: true,
        ruleName: 'trigger-battery-pipeline-' + props.resourcePrefix + '-' + props.randomSTR,
        schedule: Schedule.rate(Duration.days(1)),
    });
    triggerBatteryPipeline.addTarget(new SfnStateMachine(batteryPipeline, {
      input: RuleTargetInput.fromObject({
        "BATTERY_DATASETS": env["BATTERY_DATASETS"]
      })
    }));
  }
}