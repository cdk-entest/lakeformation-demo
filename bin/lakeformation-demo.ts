#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { LakeFormationStack } from "../lib/lakeformation-stack";
import { DataScientistStack } from "../lib/data-scientist-stack";
import { config } from "../config";
import { DatabasePermission } from "../lib/permission-type";
import {
  DataEngineerStack,
  LFWorkFlowRoleStack,
} from "../lib/data-engineer-stack";
import { LakeFormationTagStack } from "../lib/lakeformation-tag-stack";

const app = new cdk.App();

// create lakeformation
const lake = new LakeFormationStack(app, "LakeFormationStack", {
  s3LakeName: config.s3LakeName,
  athenaBucket: config.athenaResultBucket,
  registerBuckets: ["amazon-reviews-pds"],
});

// create a data scientist
const ds = new DataScientistStack(app, "DataScientistStack", {
  userName: "data-scientist",
  athenaResultBucket: config.athenaResultBucket,
  databaseName: "default",
  databasePermissions: [DatabasePermission.All],
});

// data engineer etl with glue role
const de = new DataEngineerStack(app, "DataEngineerStack", {
  sourceBucketArn: "arn:aws:s3:::amazon-reviews-pds",
  destBucketArn: `arn:aws:s3:::${config.s3LakeName}`,
  databaseName: "default",
  env: {
    region: process.env.CDK_DEFAULT_REGION,
    account: process.env.CDK_DEFAULT_ACCOUNT,
  },
});

// lakeforamtion workflow role
const workflow = new LFWorkFlowRoleStack(app, "LFWorkFlowRoleStack", {
  sourceBucketArn: "arn:aws:s3:::amazon-reviews-pds",
  destBucketArn: `arn:aws:s3:::${config.s3LakeName}`,
  databaseName: "default",
});

// lakeformation tag 
const tag = new LakeFormationTagStack(app, "LakeFormationTagStack", {
  principalArn: "arn:aws:iam::455595963207:user/data-scientist",
  env: {
    region: process.env.CDK_DEFAULT_REGION,
    account: process.env.CDK_DEFAULT_ACCOUNT
  }
})


ds.addDependency(lake);
de.addDependency(lake);
workflow.addDependency(lake);
tag.addDependency(ds)
