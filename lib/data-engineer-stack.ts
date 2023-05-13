import { Stack, StackProps, aws_iam, aws_lakeformation } from "aws-cdk-lib";
import { Effect } from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";

interface DataEngineerRoleProps extends StackProps {
  sourceBucketArn: string;
  destBucketArn: string;
  databaseName: string;
}

export class DataEngineerStack extends Stack {
  constructor(scope: Construct, id: string, props: DataEngineerRoleProps) {
    super(scope, id, props);

    // glue role
    const role = new aws_iam.Role(this, "RoleForDataEngineer", {
      roleName: "RoleForDataEngineer",
      assumedBy: new aws_iam.ServicePrincipal("glue.amazonaws.com"),
    });

    role.addManagedPolicy(
      aws_iam.ManagedPolicy.fromAwsManagedPolicyName(
        "service-role/AWSGlueServiceRole"
      )
    );

    role.addManagedPolicy(
      aws_iam.ManagedPolicy.fromAwsManagedPolicyName(
        "CloudWatchAgentServerPolicy"
      )
    );

    // manual setting iam policy for glue role
    const policy = new aws_iam.Policy(this, "PolicyForDataEngineerRole", {
      policyName: "PolicyForDataEngineerRole",
      statements: [
        // pass iam role or follow name convention
        // AWSGlueServiceRoleDefault
        new aws_iam.PolicyStatement({
          actions: ["iam:PassRole", "iam:GetRole"],
          effect: Effect.ALLOW,
          resources: ["*"],
        }),
        // access s3
        new aws_iam.PolicyStatement({
          actions: ["s3:*"],
          effect: Effect.ALLOW,
          resources: [
            props.sourceBucketArn,
            `${props.sourceBucketArn}/*`,
            props.destBucketArn,
            `${props.destBucketArn}/*`,
          ],
        }),
        // access glue
        new aws_iam.PolicyStatement({
          actions: ["glue:*"],
          effect: Effect.ALLOW,
          resources: [
            `arn:aws:glue:${this.region}:*:table/${props.databaseName}/*`,
            `arn:aws:glue:${this.region}:*:database/${props.databaseName}`,
            `arn:aws:glue:${this.region}:*:catalog`,
          ],
        }),
        // lakeformation
        new aws_iam.PolicyStatement({
          actions: [
            "lakeforamtion:GetDataAccess",
            "lakeforamtion:GrantPermissions",
          ],
          resources: ["*"],
          effect: Effect.ALLOW,
        }),
      ],
    });
    policy.attachToRole(role);
  }
}

interface LFWorkFlowRoleProps extends StackProps {
  sourceBucketArn: string;
  destBucketArn: string;
  databaseName: string;
}

export class LFWorkFlowRoleStack extends Stack {
  constructor(scope: Construct, id: string, props: LFWorkFlowRoleProps) {
    super(scope, id, props);

    // glue role
    const role = new aws_iam.Role(this, "LakeFormationWorkFlowRole", {
      roleName: "LakeFormationWorkFlowRole",
      assumedBy: new aws_iam.ServicePrincipal("glue.amazonaws.com"),
    });

    // remove this and check
    role.addManagedPolicy(
      aws_iam.ManagedPolicy.fromAwsManagedPolicyName(
        "service-role/AWSGlueServiceRole"
      )
    );

    role.addManagedPolicy(
      aws_iam.ManagedPolicy.fromAwsManagedPolicyName(
        "CloudWatchAgentServerPolicy"
      )
    );

    // manual setting iam policy for glue role
    const policy = new aws_iam.Policy(this, "PolicyForLakeFormationWorkFlow", {
      policyName: "PolicyForLakeFormationWorkFlow",
      statements: [
        // pass iam role or follow name convention
        // AWSGlueServiceRoleDefault
        new aws_iam.PolicyStatement({
          actions: ["iam:PassRole", "iam:GetRole"],
          effect: Effect.ALLOW,
          resources: ["*"],
        }),
        // access s3
        new aws_iam.PolicyStatement({
          actions: ["s3:*"],
          effect: Effect.ALLOW,
          resources: [
            props.sourceBucketArn,
            `${props.sourceBucketArn}/*`,
            // props.destBucketArn,
            // `${props.destBucketArn}/*`,
          ],
        }),
        new aws_iam.PolicyStatement({
          actions: [
            "lakeformation:GetDataAccess",
            "lakeformation:GrantPermissions",
          ],
          effect: Effect.ALLOW,
          resources: ["*"],
        }),
      ],
    });
    policy.attachToRole(role);

    const grant = new aws_lakeformation.CfnPrincipalPermissions(
      this,
      "GrantLocationPermissionGlueRole-1",
      {
        permissions: ["DATA_LOCATION_ACCESS"],
        permissionsWithGrantOption: ["DATA_LOCATION_ACCESS"],
        principal: {
          dataLakePrincipalIdentifier: role.roleArn,
        },
        resource: {
          dataLocation: {
            catalogId: this.account,
            resourceArn: `${props.destBucketArn}`,
          },
        },
      }
    );

    grant.addDependency(role.node.defaultChild as aws_iam.CfnRole);
  }
}
