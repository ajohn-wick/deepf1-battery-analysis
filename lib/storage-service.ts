/***** CDK *****/
import { Construct } from 'constructs';
import { RemovalPolicy } from 'aws-cdk-lib';
/***** END CDK *****/

import { Bucket, BucketEncryption } from 'aws-cdk-lib/aws-s3';

export interface StorageServiceProps {
  appPrefix: string;
  resourcePrefix: string;
  awsAccount: string;
  awsRegion: string;
  randomSTR: string;
}

export class StorageService extends Construct {

  private s3RAWBucket: Bucket;
  private s3STDBucket: Bucket;

  public getS3RAWBucket(): Bucket {
    return this.s3RAWBucket;
  }
  public getS3STDBucket(): Bucket {
    return this.s3STDBucket;
  }

  constructor(scope: Construct, id: string, props: StorageServiceProps) {
    super(scope, id);

    this.s3RAWBucket = new Bucket(this, props.appPrefix + 'RAWBucket', {
      bucketName: props.resourcePrefix + '-raw-' + props.randomSTR,
      encryption: BucketEncryption.KMS_MANAGED,
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY
    });
    this.s3STDBucket = new Bucket(this, props.appPrefix + 'STDBucket', {
      bucketName: props.resourcePrefix + '-std-' + props.randomSTR,
      encryption: BucketEncryption.KMS_MANAGED,
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY,
    });
  }
}