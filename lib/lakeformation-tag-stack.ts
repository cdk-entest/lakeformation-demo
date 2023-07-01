import { Stack, StackProps, aws_lakeformation } from "aws-cdk-lib";
import { Construct } from "constructs";


interface LakeFormationTagProps extends StackProps {
    databaseName?: string
    tableName?: string
    columnNames?: string[]
    principalArn: string
}

export class LakeFormationTagStack extends Stack {

    constructor(scope: Construct, id: string, { databaseName = "default", tableName = "amazon_review_table", columnNames = ["marketplace", "customer_id"], principalArn }: LakeFormationTagProps) {
        super(scope, id)

        // create environment tag 
        const envTag = new aws_lakeformation.CfnTag(
            this,
            "EnvironmentTag",
            {
                tagKey: "environment",
                tagValues: ["production"]
            }
        )

        // create privacy tag 
        const privacyTag = new aws_lakeformation.CfnTag(
            this,
            "PrivacyTag",
            {
                tagKey: "privacy",
                tagValues: ["open"]
            }
        )

        // associate privacy tag  
        const associatePrivacy = new aws_lakeformation.CfnTagAssociation(
            this,
            "PrivacyTagAssociation",
            {
                lfTags: [{
                    catalogId: this.account,
                    tagKey: privacyTag.tagKey,
                    tagValues: privacyTag.tagValues
                }],
                resource: {
                    tableWithColumns: {
                        catalogId: this.account,
                        columnNames: ["product_id", "product_title"],
                        databaseName: databaseName,
                        name: tableName
                    }
                },
            }
        )

        // associate environment tag 
        const associateEnvironment = new aws_lakeformation.CfnTagAssociation(
            this,
            "EnvironmentTagAssociation",
            {
                lfTags: [{
                    catalogId: this.account,
                    tagKey: envTag.tagKey,
                    tagValues: envTag.tagValues,
                }],
                resource: {
                    table: {
                        catalogId: this.account,
                        databaseName: databaseName,
                        name: tableName,
                        tableWildcard: {}
                    }
                }
            }
        )

        // tag data scientist 
        new aws_lakeformation.CfnPrincipalPermissions(
            this,
            "TagDataScientist",
            {
                permissions: ["DESCRIBE", "SELECT"],
                permissionsWithGrantOption: ["DESCRIBE"],
                principal: {
                    dataLakePrincipalIdentifier: principalArn
                },
                resource: {
                    lfTagPolicy: {
                        catalogId: this.account,
                        expression: [{
                            tagKey: privacyTag.tagKey,
                            tagValues: privacyTag.tagValues
                        }],
                        resourceType: "TABLE"
                    }
                }
            }
        )

        // only support SELECT 
        new aws_lakeformation.CfnPrincipalPermissions(
            this,
            "TableWithColumnsResourceSelectOnly",
            {
                permissions: ["SELECT"],
                permissionsWithGrantOption: ["SELECT"],
                principal: {
                    dataLakePrincipalIdentifier: principalArn
                },
                resource: {
                    tableWithColumns: {
                        catalogId: this.account,
                        databaseName: databaseName,
                        name: tableName,
                        columnNames: columnNames,
                    }
                }
            }
        )

        associatePrivacy.addDependency(associateEnvironment)
    }
}