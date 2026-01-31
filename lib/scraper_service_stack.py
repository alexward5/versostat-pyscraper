"""Scraper service stack: ECS task, Step Functions, EventBridge schedule, SNS failures."""
from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import (
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_logs as logs,
    aws_secretsmanager as secrets,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct


class ScraperServiceStack(cdk.Stack):
    """Stack for scraper ECS tasks orchestrated by Step Functions."""

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        env: cdk.Environment | None = None,
        sportmonks_secret_arn: str | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, id, env=env, **kwargs)

        if not sportmonks_secret_arn:
            raise ValueError(
                "sportmonksSecretArn required in CDK context. "
                "Create the secret first, then: cdk deploy -c sportmonksSecretArn=arn:aws:secretsmanager:..."
            )

        # ---- Imports from other stacks ----
        vpc_id = cdk.Fn.import_value("VersoStat-VpcId")
        azs_ref = cdk.Fn.import_value("VersoStat-AvailabilityZones")
        public_subnet_ids_ref = cdk.Fn.import_value("VersoStat-PublicSubnetIds")
        cluster_name = cdk.Fn.import_value("VersoStat-ClusterName")
        cluster_arn = cdk.Fn.import_value("VersoStat-ClusterArn")
        ecr_uri = cdk.Fn.import_value("VersoStat-PyscraperEcrRepositoryUri")
        scraper_sg_id = cdk.Fn.import_value("VersoStat-ScraperTaskSecurityGroupId")
        db_sg_id = cdk.Fn.import_value("VersoStat-DbSecurityGroupId")
        db_host = cdk.Fn.import_value("VersoStat-DbHost")
        db_port = cdk.Fn.import_value("VersoStat-DbPort")
        db_secret_arn = cdk.Fn.import_value("VersoStat-DbSecretArn")

        vpc = ec2.Vpc.from_vpc_attributes(
            self,
            "ImportedVpc",
            vpc_id=vpc_id,
            availability_zones=cdk.Fn.split(",", azs_ref),
            public_subnet_ids=cdk.Fn.split(",", public_subnet_ids_ref),
        )

        cluster = ecs.Cluster.from_cluster_attributes(
            self,
            "ImportedCluster",
            cluster_name=cluster_name,
            cluster_arn=cluster_arn,
            vpc=vpc,
        )

        scraper_sg = ec2.SecurityGroup.from_security_group_id(
            self,
            "ScraperSg",
            scraper_sg_id,
        )
        db_sg = ec2.SecurityGroup.from_security_group_id(
            self,
            "DbSg",
            db_sg_id,
        )

        # Allow scraper tasks to reach RDS
        db_sg.add_ingress_rule(
            scraper_sg,
            ec2.Port.tcp(5432),
            "Scraper tasks to RDS 5432",
        )

        # ---- Log group ----
        log_group = logs.LogGroup(
            self,
            "PyscraperLogGroup",
            retention=logs.RetentionDays.TWO_WEEKS,
        )

        # ---- Task definition ----
        task_def = ecs.FargateTaskDefinition(
            self,
            "ScraperTaskDef",
            cpu=256,
            memory_limit_mib=512,
            runtime_platform=ecs.RuntimePlatform(
                cpu_architecture=ecs.CpuArchitecture.X86_64,
                operating_system_family=ecs.OperatingSystemFamily.LINUX,
            ),
        )

        task_def.obtain_execution_role().add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AmazonECSTaskExecutionRolePolicy"
            )
        )

        db_secret = secrets.Secret.from_secret_complete_arn(
            self,
            "DbSecret",
            db_secret_arn,
        )
        sportmonks_secret = secrets.Secret.from_secret_complete_arn(
            self,
            "SportmonksSecret",
            sportmonks_secret_arn,
        )

        image_tag = self.node.try_get_context("imageTag") or "latest"
        container = task_def.add_container(
            "scraper",
            image=ecs.ContainerImage.from_registry(f"{ecr_uri}:{image_tag}"),
            logging=ecs.LogDriver.aws_logs(
                log_group=log_group,
                stream_prefix="scraper",
            ),
            environment={
                "DB_HOST": db_host,
                "DB_PORT": db_port,
                "DB_NAME": "versostat_db",
                "PGSSLMODE": "verify-full",
                "PGSSLROOTCERT": "/etc/ssl/certs/rds-global-bundle.pem",
                "PGHOST": db_host,
                "PGPORT": db_port,
                "PGDATABASE": "versostat_db",
            },
            secrets={
                "DB_USER": ecs.Secret.from_secrets_manager(db_secret, "username"),
                "DB_PASSWORD": ecs.Secret.from_secrets_manager(db_secret, "password"),
                "SPORTMONKS_API_KEY": ecs.Secret.from_secrets_manager(
                    sportmonks_secret,
                ),
            },
        )

        # ---- ECS RunTask helpers ----
        def make_run_task(scripts: str) -> tasks.EcsRunTask:
            """Create ECS RunTask with schema from input and given scripts."""
            task = tasks.EcsRunTask(
                self,
                f"Run{scripts.replace('_', '').title()}",
                cluster=cluster,
                task_definition=task_def,
                assign_public_ip=True,
                container_overrides=[
                    tasks.ContainerOverride(
                        container_definition=container,
                        command=["python", "index.py", "--scripts", scripts],
                        environment=[
                            tasks.TaskEnvironmentVariable(
                                name="SCRIPT_SCHEMA",
                                value=sfn.JsonPath.string_at("$.schema"),
                            )
                        ],
                    )
                ],
                integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                launch_target=tasks.EcsFargateLaunchTarget(
                    platform_version=ecs.FargatePlatformVersion.LATEST
                ),
                security_groups=[scraper_sg],
                subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
                result_path="$.taskResult",
            )
            task.add_retry(max_attempts=0)
            return task

        # ---- Step Function: Pass (ensures schema in output) -> Parallel -> Crosswalk -> Views ----
        # Pass state propagates schema so it survives ECS RunTask output replacement.
        setup_input = sfn.Pass(
            self,
            "SetupInput",
            parameters={"schema": "my_schema"},
        )

        run_fpl = make_run_task("fpl")
        run_sm = make_run_task("sm")
        parallel = sfn.Parallel(
            self,
            "ParallelFplSm",
            result_path=sfn.JsonPath.DISCARD,
        )
        parallel.branch(run_fpl)
        parallel.branch(run_sm)

        run_crosswalk = make_run_task("crosswalk_player_id")
        run_views = make_run_task("views")

        # SNS topic for failures
        failure_topic = sns.Topic(
            self,
            "ScraperFailureTopic",
            display_name="VersoStat Scraper Failures",
        )
        failure_topic.add_subscription(
            subs.EmailSubscription("alexanderward5@gmail.com")
        )

        # Chain: setup -> parallel -> crosswalk -> views
        definition = (
            setup_input
            .next(parallel)
            .next(run_crosswalk)
            .next(run_views)
        )

        state_machine = sfn.StateMachine(
            self,
            "VersoStatScraper",
            state_machine_name="VersoStatScraper",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=cdk.Duration.hours(2),
        )

        # EventBridge rule: Step Functions FAILED -> SNS
        events.Rule(
            self,
            "ScraperFailureRule",
            description="Notify on scraper Step Function failure",
            event_pattern=events.EventPattern(
                source=["aws.states"],
                detail_type=["Step Functions Execution Status Change"],
                detail={
                    "status": ["FAILED"],
                    "stateMachineArn": [state_machine.state_machine_arn],
                },
            ),
            targets=[targets.SnsTopic(failure_topic)],
        )

        # EventBridge schedule: 01:30 UTC daily
        events.Rule(
            self,
            "ScraperScheduleRule",
            schedule=events.Schedule.cron(
                minute="30",
                hour="1",
                month="*",
                week_day="*",
                year="*",
            ),
            targets=[
                targets.SfnStateMachine(
                    state_machine,
                    input=events.RuleTargetInput.from_object({"schema": "my_schema"}),
                )
            ],
        )

        # Outputs
        cdk.CfnOutput(
            self,
            "StateMachineArn",
            value=state_machine.state_machine_arn,
            export_name="VersoStat-ScraperStateMachineArn",
        )
        cdk.CfnOutput(
            self,
            "LogGroupName",
            value=log_group.log_group_name,
            export_name="VersoStat-PyscraperLogGroupName",
        )
