/***** CDK *****/
import { Construct } from 'constructs';
import { Duration } from 'aws-cdk-lib';
/***** END CDK *****/

import { Role, ServicePrincipal, PolicyStatement, Effect, Policy } from 'aws-cdk-lib/aws-iam';
import { Runtime, Function } from 'aws-cdk-lib/aws-lambda';
import { PythonFunction } from '@aws-cdk/aws-lambda-python-alpha';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { CfnCrawler } from 'aws-cdk-lib/aws-glue';
const env = require('../resources/config/env.json');

export interface MicroServiceProps {
  appPrefix: string;
  resourcePrefix: string;
  awsAccount: string;
  awsRegion: string;
  randomSTR: string;
  s3RAWBucket: Bucket;
  glueSTDCrawler: CfnCrawler;
}

export class MicroService extends Construct {

  private lambdaIngestRAW: Function;
  private lambdaListRAW: Function;
  private lambdaRunSTDCrawler: Function;
  private lambdaCheckSTDCrawler: Function;

  public getLambdaIngestRAWFunction(): Function {
    return this.lambdaIngestRAW;
  }
  public getLambdaListRAWFunction(): Function {
    return this.lambdaListRAW;
  }
  public getLambdaRunSTDCrawlerFunction(): Function {
    return this.lambdaRunSTDCrawler;
  }
  public getLambdaCheckSTDCrawlerFunction(): Function {
    return this.lambdaCheckSTDCrawler;
  }

  constructor(scope: Construct, id: string, props: MicroServiceProps) {
    super(scope, id);
    let s3Prefix = env["S3_PREFIX"]
    if (s3Prefix.slice(-1) != '/') {
      s3Prefix += '/'
    }

    /***** INGEST RAW LAMBDA RESOURCES *****/
    const iamIngestRAWLambdaRole = new Role(this, props.appPrefix + 'IAMIngestRAWLambdaRole', {
      roleName: props.resourcePrefix + '-lambda-ingest-raw',
      assumedBy: new ServicePrincipal('lambda.amazonaws.com')
    });

    this.lambdaIngestRAW = new PythonFunction(this, props.appPrefix + 'IngestRAWLambda', {
      functionName: props.resourcePrefix + '-get-battery-' + props.randomSTR,
      entry: './resources/lambda',
      index: 'ingest_raw.py',
      handler: 'handler',
      runtime: Runtime.PYTHON_3_7,
      timeout: Duration.seconds(60),
      memorySize: 256,
      role: iamIngestRAWLambdaRole,
      environment: {
        DATASETS_ENDPOINT: env["DATASETS_ENDPOINT"],
        S3_RAW_BUCKET_NAME: props.s3RAWBucket.bucketName,
        S3_PREFIX: s3Prefix
      }
    });

    const cwIngestLambdaRAWLogGroup = new LogGroup(this, props.appPrefix + 'CWLambdaIngestRAWLogGroup', {
      logGroupName: '/aws/lambda/' + this.lambdaIngestRAW.functionName,
      retention: RetentionDays.TWO_WEEKS
    });

    const iamLambdaIngestRAWCWPolicy = new Policy(this, props.appPrefix + 'IAMLambdaIngestRAWCWPolicy', {
      policyName: props.resourcePrefix + '-lambda-cw-ingest-raw-policy',
      statements: [new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'logs:CreateLogStream',
          'logs:PutLogEvents'
        ],
        resources: [cwIngestLambdaRAWLogGroup.logGroupArn]
      })]
    });
    iamLambdaIngestRAWCWPolicy.attachToRole(iamIngestRAWLambdaRole);

    const iamLambdaS3IngestRAWPolicy = new Policy(this, props.appPrefix + 'IAMLambdaS3IngestRAWPolicy', {
      policyName: props.resourcePrefix + '-lambda-s3-ingest-raw-policy',
      statements: [new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:PutObject*'
        ],
        resources: [props.s3RAWBucket.bucketArn
          , props.s3RAWBucket.bucketArn + '/*'
        ]
      })]
    });
    iamLambdaS3IngestRAWPolicy.attachToRole(iamIngestRAWLambdaRole);
    /***** END INGEST RAW LAMBDA RESOURCES *****/

    /***** LIST RAW LAMBDA RESOURCES *****/
    const iamListRAWLambdaRole = new Role(this, props.appPrefix + 'IAMListRAWLambdaRole', {
      roleName: props.resourcePrefix + '-lambda-list-raw',
      assumedBy: new ServicePrincipal('lambda.amazonaws.com')
    });

    this.lambdaListRAW = new PythonFunction(this, props.appPrefix + 'ListRAWLambda', {
      functionName: props.resourcePrefix + '-list-battery-' + props.randomSTR,
      entry: './resources/lambda',
      index: 'list_raw.py',
      handler: 'handler',
      runtime: Runtime.PYTHON_3_7,
      timeout: Duration.seconds(15),
      memorySize: 128,
      role: iamListRAWLambdaRole,
      environment: {
        S3_RAW_BUCKET_NAME: props.s3RAWBucket.bucketName,
        S3_PREFIX: s3Prefix
      }
    });

    const cwListLambdaRAWLogGroup = new LogGroup(this, props.appPrefix + 'CWLambdaListRAWLogGroup', {
      logGroupName: '/aws/lambda/' + this.lambdaListRAW.functionName,
      retention: RetentionDays.TWO_WEEKS
    });

    const iamLambdaListRAWCWPolicy = new Policy(this, props.appPrefix + 'IAMLambdaListRAWCWPolicy', {
      policyName: props.resourcePrefix + '-lambda-cw-list-raw-policy',
      statements: [new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'logs:CreateLogStream',
          'logs:PutLogEvents'
        ],
        resources: [cwListLambdaRAWLogGroup.logGroupArn]
      })]
    });
    iamLambdaListRAWCWPolicy.attachToRole(iamListRAWLambdaRole);

    const iamLambdaS3ListRAWPolicy = new Policy(this, props.appPrefix + 'IAMLambdaS3ListRAWPolicy', {
      policyName: props.resourcePrefix + '-lambda-s3-list-raw-policy',
      statements: [new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:HeadObject',
          's3:AbortMultipartUpload',
          's3:ListBucket*',
          's3:GetBucket*',
          's3:GetObject*',
          's3:ListMultipartUploadParts'
        ],
        resources: [props.s3RAWBucket.bucketArn
          , props.s3RAWBucket.bucketArn + '/' + s3Prefix + '*'
        ]
      })]
    });
    iamLambdaS3ListRAWPolicy.attachToRole(iamListRAWLambdaRole);
    /***** END LIST RAW LAMBDA RESOURCES *****/

    /***** RUN STD CRAWLER LAMBDA RESOURCES *****/
    const iamRunSTDCrawlerLambdaRole = new Role(this, props.appPrefix + 'IAMRunSTDCrawlerLambdaRole', {
      roleName: props.resourcePrefix + '-lambda-run-std-crawler',
      assumedBy: new ServicePrincipal('lambda.amazonaws.com')
    });

    this.lambdaRunSTDCrawler = new PythonFunction(this, props.appPrefix + 'RunSTDCrawlerLambda', {
      functionName: props.resourcePrefix + '-run-std-crawler-' + props.randomSTR,
      entry: './resources/lambda',
      index: 'run_crawler.py',
      handler: 'handler',
      runtime: Runtime.PYTHON_3_7,
      timeout: Duration.seconds(15),
      memorySize: 128,
      role: iamRunSTDCrawlerLambdaRole,
      environment: {
        CRAWLER_NAME: props.glueSTDCrawler.ref
      }
    });

    const cwLambdaRunSTDCrawlerLogGroup = new LogGroup(this, props.appPrefix + 'CWLambdaRunSTDCrawlerLogGroup', {
      logGroupName: '/aws/lambda/' + this.lambdaRunSTDCrawler.functionName,
      retention: RetentionDays.TWO_WEEKS
    });

    const iamLambdaRunSTDCrawlerCWPolicy = new Policy(this, props.appPrefix + 'IAMLambdaRunSTDCrawlerCWPolicy', {
      policyName: props.resourcePrefix + '-lambda-cw-run-std-crawler-policy',
      statements: [new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'logs:CreateLogStream',
          'logs:PutLogEvents'
        ],
        resources: [cwLambdaRunSTDCrawlerLogGroup.logGroupArn]
      })]
    });
    iamLambdaRunSTDCrawlerCWPolicy.attachToRole(iamRunSTDCrawlerLambdaRole);

    const iamLambdaGlueRunCrawlerPolicy = new Policy(this, props.appPrefix + 'IAMLambdaGlueRunSTDCrawlerPolicy', {
      policyName: props.resourcePrefix + '-lambda-glue-run-std-crawler-policy',
      statements: [new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'glue:StartCrawler'
        ],
        resources: [
          'arn:aws:glue:' + props.awsRegion + ':' + props.awsAccount + ':crawler/' + props.glueSTDCrawler.ref
        ]
      })]
    });
    iamLambdaGlueRunCrawlerPolicy.attachToRole(iamRunSTDCrawlerLambdaRole);
    /***** END RUN STD CRAWLER LAMBDA RESOURCES *****/

    /***** CHECK STD CRAWLER LAMBDA RESOURCES *****/
    const iamCheckSTDCrawlerLambdaRole = new Role(this, props.appPrefix + 'IAMCheckSTDCrawlerLambdaRole', {
      roleName: props.resourcePrefix + '-lambda-check-std-crawler',
      assumedBy: new ServicePrincipal('lambda.amazonaws.com')
    });

    this.lambdaCheckSTDCrawler = new PythonFunction(this, props.appPrefix + 'CheckSTDCrawlerLambda', {
      functionName: props.resourcePrefix + '-check-std-crawler-' + props.randomSTR,
      entry: './resources/lambda',
      index: 'check_crawler.py',
      handler: 'handler',
      runtime: Runtime.PYTHON_3_7,
      timeout: Duration.seconds(15),
      memorySize: 128,
      role: iamCheckSTDCrawlerLambdaRole,
      environment: {
        CRAWLER_NAME: props.glueSTDCrawler.ref
      }
    });

    const cwLambdaCheckSTDCrawlerLogGroup = new LogGroup(this, props.appPrefix + 'CWLambdaCheckSTDCrawlerLogGroup', {
      logGroupName: '/aws/lambda/' + this.lambdaCheckSTDCrawler.functionName,
      retention: RetentionDays.TWO_WEEKS
    });

    const iamLambdaCheckSTDCrawlerCWPolicy = new Policy(this, props.appPrefix + 'IAMLambdaCheckSTDCrawlerCWPolicy', {
      policyName: props.resourcePrefix + '-lambda-cw-check-std-crawler-policy',
      statements: [new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'logs:CreateLogStream',
          'logs:PutLogEvents'
        ],
        resources: [cwLambdaCheckSTDCrawlerLogGroup.logGroupArn]
      })]
    });
    iamLambdaCheckSTDCrawlerCWPolicy.attachToRole(iamCheckSTDCrawlerLambdaRole);

    const iamLambdaGlueCheckSTDCrawlerPolicy = new Policy(this, props.appPrefix + 'IAMLambdaGlueCheckSTDCrawlerPolicy', {
      policyName: props.resourcePrefix + '-lambda-glue-check-std-crawler-policy',
      statements: [new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          'glue:GetCrawler'
        ],
        resources: [
          'arn:aws:glue:' + props.awsRegion + ':' + props.awsAccount + ':crawler/' + props.glueSTDCrawler.ref
        ]
      })]
    });
    iamLambdaGlueCheckSTDCrawlerPolicy.attachToRole(iamCheckSTDCrawlerLambdaRole);
    /***** END CHECK STD CRAWLER LAMBDA RESOURCES *****/
  }
}