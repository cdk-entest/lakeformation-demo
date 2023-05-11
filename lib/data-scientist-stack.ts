import {
  aws_iam,
  aws_lakeformation,
  aws_secretsmanager,
  Stack,
  StackProps,
} from "aws-cdk-lib";
import { Effect } from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";

interface DataScientistProps extends StackProps {
  userName: string;
  athenaResultBucket: string;
  databaseName: string;
  databasePermissions: string[];
}

export class DataScientistStack extends Stack {
  public userArn: string;

  constructor(scope: Construct, id: string, props: DataScientistProps) {
    super(scope, id, props);

    const secret = new aws_secretsmanager.Secret(
      this,
      `${props.userName}Secret`,
      {
        secretName: `${props.userName}Secret`,
        generateSecretString: {
          secretStringTemplate: JSON.stringify({ username: props.userName }),
          generateStringKey: "password",
        },
      }
    );

    // create an iam user for data analyst (da)
    const daUser = new aws_iam.User(this, `${props.userName}IAMUser`, {
      userName: props.userName,
      password: secret.secretValueFromJson("password"),
      passwordResetRequired: false,
    });

    // data scientist write to query result bucket
    daUser.addToPolicy(
      new aws_iam.PolicyStatement({
        effect: Effect.ALLOW,
        actions: ["s3:*"],
        resources: [
          `arn:aws:s3:::${props.athenaResultBucket}/*`,
          `arn:aws:s3:::${props.athenaResultBucket}`,
        ],
      })
    );

    // data scientst can use quicksight
    daUser.addToPolicy(
      new aws_iam.PolicyStatement({
        effect: Effect.ALLOW,
        actions: ["quicksight:*"],
        resources: ["*"],
      })
    );

    // get access from lakeformation
    daUser.addToPolicy(
      new aws_iam.PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "lakeformation:GetDataAccess",
          "glue:GetTable",
          "glue:GetTables",
          "glue:SearchTables",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetPartitions",
          "lakeformation:GetResourceLFTags",
          "lakeformation:ListLFTags",
          "lakeformation:GetLFTag",
          "lakeformation:SearchTablesByLFTags",
          "lakeformation:SearchDatabasesByLFTags",
        ],
        resources: ["*"],
      })
    );

    // coarse grant permission by iam
    daUser.addManagedPolicy(
      aws_iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonAthenaFullAccess")
    );

    // fine grant permission by lakeformation
    const tablePermission = new aws_lakeformation.CfnPrincipalPermissions(
      this,
      `${props.userName}-ReadTableLake-2`,
      {
        permissions: props.databasePermissions,
        permissionsWithGrantOption: props.databasePermissions,
        principal: {
          dataLakePrincipalIdentifier: daUser.userArn,
        },
        resource: {
          table: {
            catalogId: this.account,
            databaseName: props.databaseName,
            tableWildcard: {},
          },
        },
      }
    );

    new aws_lakeformation.CfnPrincipalPermissions(
      this,
      `${props.userName}-ReadTableLake-4`,
      {
        permissions: props.databasePermissions,
        permissionsWithGrantOption: props.databasePermissions,
        principal: {
          dataLakePrincipalIdentifier: daUser.userArn,
        },
        resource: {
          database: {
            catalogId: this.account,
            name: props.databaseName,
          },
        },
      }
    );

    tablePermission.addDependency(daUser.node.defaultChild as aws_iam.CfnUser);

    // setting result query S3 prefix
    this.userArn = this.userArn;
  }
}
