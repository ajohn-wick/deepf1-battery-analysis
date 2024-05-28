/***** CDK *****/
import { Construct } from 'constructs';
import { Duration } from 'aws-cdk-lib';
/***** END CDK *****/

import { Role, ServicePrincipal, PolicyStatement, Effect, Policy, ManagedPolicy } from 'aws-cdk-lib/aws-iam';
import { Job, JobExecutable, GlueVersion, PythonVersion, Code, WorkerType, Database } from '@aws-cdk/aws-glue-alpha';
import { CfnCrawler } from 'aws-cdk-lib/aws-glue';
import { Bucket } from 'aws-cdk-lib/aws-s3';

export interface DataServiceProps {
  appPrefix: string;
  resourcePrefix: string;
  awsAccount: string;
  awsRegion: string;
  randomSTR: string;
  s3RAWBucket: Bucket;
  s3STDBucket: Bucket;
}

export class DataService extends Construct {

  private glueSTDJob: Job;
  private glueSTDCrawler: CfnCrawler;

  public getGlueSTDJob(): Job {
    return this.glueSTDJob;
  }
  public getGlueSTDCrawler(): CfnCrawler {
    return this.glueSTDCrawler;
  }

  constructor(scope: Construct, id: string, props: DataServiceProps) {
    super(scope, id);

    const glueSTDDB = new Database(this, props.appPrefix + 'STDGlueDB', {
      databaseName: props.resourcePrefix + '-standardized',
      locationUri: 's3://' + props.s3STDBucket.bucketName + '/'
    });
    
    /***** GLUE STD JOB RESOURCES *****/
    const iamSTDGlueJobRole = new Role(this, props.appPrefix + 'IAMSTDGlueJobRole', {
      roleName: props.resourcePrefix + '-glue-job-std',
      assumedBy: new ServicePrincipal('glue.amazonaws.com')
    });

    const iamGlueJobS3RAWPolicy = new Policy(this, props.appPrefix + 'IAMGlueJobS3RAWPolicy', {
      policyName: props.resourcePrefix + '-glue-job-s3-raw-policy',
      statements: [
      new PolicyStatement({
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
          , props.s3RAWBucket.bucketArn + '/*'
        ]
      })]
    });
    iamGlueJobS3RAWPolicy.attachToRole(iamSTDGlueJobRole);

    const iamGlueJobS3STDPolicy = new Policy(this, props.appPrefix + 'IAMGlueJobS3STDPolicy', {
      policyName: props.resourcePrefix + '-glue-job-s3-std-policy',
      statements: [
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:HeadObject',
          's3:AbortMultipartUpload',
          's3:ListBucket*',
          's3:GetBucket*',
          's3:GetObject*',
          's3:PutObject*',
          's3:ListMultipartUploadParts'
        ],
        resources: [props.s3STDBucket.bucketArn
          , props.s3STDBucket.bucketArn + '/*'
        ]
      })]
    });
    iamGlueJobS3STDPolicy.attachToRole(iamSTDGlueJobRole);

    this.glueSTDJob = new Job(this, props.appPrefix + 'STDGlueJob', {
      jobName: props.resourcePrefix + '-standardize-battery-datasets-' + props.randomSTR,
      role: iamSTDGlueJobRole,
      executable: JobExecutable.pythonEtl({
        glueVersion: GlueVersion.V3_0,
        pythonVersion: PythonVersion.THREE,
        script: Code.fromAsset('./resources/glue/standardize.py')
      }),
      timeout: Duration.seconds(300),
      defaultArguments: {
        "--job-language": "python",
        // "--enable-auto-scaling": "",
        "--enable-metrics": "",
        "--enable-glue-datacatalog": "",
        "--job-bookmark-option": "job-bookmark-disable",
        "--encryption-type": "sse-s3"
      },
      workerType: WorkerType.G_2X,
      workerCount: 2,
      maxConcurrentRuns: 2
    });
    /***** END GLUE STD JOB RESOURCES *****/

    /***** GLUE STD CRAWLER RESOURCES *****/
    const iamSTDGlueCrawlerRole = new Role(this, props.appPrefix + 'IAMSTDGlueCrawlerRole', {
      roleName: props.resourcePrefix + '-glue-crawler-std',
      assumedBy: new ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')
      ]
    });

    const iamGlueCrawlerS3STDPolicy = new Policy(this, props.appPrefix + 'IAMGlueCrawlerS3STDPolicy', {
      policyName: props.resourcePrefix + '-glue-crawler-s3-std-policy',
      statements: [
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          's3:HeadObject',
          's3:AbortMultipartUpload',
          's3:ListBucket*',
          's3:GetBucket*',
          's3:GetObject*',
          's3:ListMultipartUploadParts'
        ],
        resources: [props.s3STDBucket.bucketArn
          , props.s3STDBucket.bucketArn + '/*'
        ]
      })]
    });
    iamGlueCrawlerS3STDPolicy.attachToRole(iamSTDGlueCrawlerRole);

    this.glueSTDCrawler = new CfnCrawler(this, props.appPrefix + 'STDGlueCrawler', {
      name: props.resourcePrefix + '-standardized-' + props.randomSTR,
      role: iamSTDGlueCrawlerRole.roleArn,
      databaseName: glueSTDDB.databaseName,
      targets: {
        "s3Targets": [
            {
                "exclusions": [
                    "result/**",
                    "_metadata",
                    "_metadata/**",
                    "_common_metadata",
                    "_common_metadata/**",
                ],
                "path": `${props.s3STDBucket.bucketName}/battery/`
            }
        ]
      },
      schemaChangePolicy: {
        "updateBehavior": "UPDATE_IN_DATABASE",
        "deleteBehavior": "DELETE_FROM_DATABASE"
      },
      configuration: '{"Version":1.0, "Grouping": {"TableGroupingPolicy":"CombineCompatibleSchemas"}}'
    });
    /***** END GLUE STD CRAWLER RESOURCES *****/
  }
}