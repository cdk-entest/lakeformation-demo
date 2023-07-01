import {
  aws_lakeformation,
  aws_s3,
  DefaultStackSynthesizer,
  Fn,
  RemovalPolicy,
  Stack,
  StackProps,
} from "aws-cdk-lib";
import { Construct } from "constructs";

interface LakeFormationProps extends StackProps {
  s3LakeName: string;
  athenaBucket: string;
  registerBuckets: string[];
}

export class LakeFormationStack extends Stack {
  public readonly s3Lake: aws_s3.Bucket;
  public readonly lakeCdkAmin: aws_lakeformation.CfnDataLakeSettings;

  constructor(scope: Construct, id: string, props: LakeFormationProps) {
    super(scope, id, props);

    // lake formation setting admin
    this.lakeCdkAmin = new aws_lakeformation.CfnDataLakeSettings(
      this,
      "LakeFormationAdminSetting",
      {
        admins: [
          {
            dataLakePrincipalIdentifier: Fn.sub(
              (this.synthesizer as DefaultStackSynthesizer)
                .cloudFormationExecutionRoleArn
            ),
          },
        ],
      }
    );

    // create lake S3 bucket
    const bucket = new aws_s3.Bucket(this, "S3LakeBucketDemo", {
      bucketName: props.s3LakeName,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // athena query result bucket
    const athenaBucket = new aws_s3.Bucket(this, "S3AthenaQueryResult", {
      bucketName: props.athenaBucket,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // register the bucket with lakeformation
    var registers: aws_lakeformation.CfnResource[] = [];
    props.registerBuckets.map((bucket) => {
      registers.push(
        new aws_lakeformation.CfnResource(
          this,
          `RegsiterBucketToLake-${bucket}`,
          {
            resourceArn: `arn:aws:s3:::${bucket}`,
            useServiceLinkedRole: true,
          }
        )
      );
    });

    const register = new aws_lakeformation.CfnResource(
      this,
      "RegisterLakeBucket",
      {
        resourceArn: bucket.bucketArn,
        useServiceLinkedRole: true,
      }
    );

    // export output and name
    this.s3Lake = bucket;
    register.addDependency(bucket.node.defaultChild as aws_s3.CfnBucket);
  }
}
