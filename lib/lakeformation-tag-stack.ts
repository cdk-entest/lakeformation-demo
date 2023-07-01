import { Stack, StackProps, aws_lakeformation } from "aws-cdk-lib";
import { Construct } from "constructs";
import { TablePermission } from "./permission-type";

const databaseName = "default"

export class LakeFormationTagStack extends Stack {

    constructor(scope: Construct, id: string, props: StackProps) {
        super(scope, id, props)

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
                        name: "amazon_review_table"
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
                        name: "amazon_review_table",
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
                    dataLakePrincipalIdentifier: "arn:aws:iam::455595963207:user/data-scientist"
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
                permissionsWithGrantOption: ["DESCRIBE"],
                principal: {
                    dataLakePrincipalIdentifier: "arn:aws:iam::455595963207:user/data-scientist"
                },
                resource: {
                    tableWithColumns: {
                        catalogId: this.account,
                        databaseName: databaseName,
                        name: "amazonreviewtable",
                        columnNames: ["marketplace", "customer_id"],
                    }
                }
            }
        )

        associatePrivacy.addDependency(associateEnvironment)
    }
}