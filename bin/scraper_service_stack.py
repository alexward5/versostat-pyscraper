#!/usr/bin/env python3
"""CDK app entry point for ScraperServiceStack."""
import sys
from pathlib import Path

# Add project root so "lib" can be imported when CDK runs this script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import aws_cdk as cdk
from lib.scraper_service_stack import ScraperServiceStack

app = cdk.App()
cfg = app.node.try_get_context("envs") or {}
prod = cfg.get("prod", {})
env = cdk.Environment(
    account=prod.get("account", "586999013151"),
    region=prod.get("region", "us-east-1"),
)
sportmonks_secret_arn = app.node.try_get_context("sportmonksSecretArn")

ScraperServiceStack(
    app,
    "VersoStat-ScraperServiceStack-prod",
    env=env,
    sportmonks_secret_arn=sportmonks_secret_arn,
)

app.synth()
