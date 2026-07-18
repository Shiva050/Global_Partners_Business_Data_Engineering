#!/usr/bin/env bash
# Create the Step Functions IAM role + state machine for the medallion pipeline.
# Usage:  cd orchestration && bash create_state_machine.sh
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
ROLE_NAME="gpb-sfn-pipeline-role"
SM_NAME="gpb-medallion-pipeline"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> [1/3] IAM role for Step Functions"
aws iam create-role --role-name "$ROLE_NAME" \
  --assume-role-policy-document "file://$HERE/iam/sfn-trust-policy.json" >/dev/null 2>&1 || echo "   role exists"
aws iam put-role-policy --role-name "$ROLE_NAME" \
  --policy-name gpb-sfn-run-glue \
  --policy-document "file://$HERE/iam/sfn-permissions-policy.json"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
echo "   $ROLE_ARN"
sleep 8   # let the role propagate before Step Functions validates it

echo "==> [2/3] Create/update state machine"
if aws stepfunctions describe-state-machine \
     --state-machine-arn "arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:${SM_NAME}" >/dev/null 2>&1; then
  aws stepfunctions update-state-machine \
    --state-machine-arn "arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:${SM_NAME}" \
    --definition "file://$HERE/state_machine.asl.json" --role-arn "$ROLE_ARN" >/dev/null
  echo "   updated"
else
  aws stepfunctions create-state-machine --name "$SM_NAME" \
    --definition "file://$HERE/state_machine.asl.json" --role-arn "$ROLE_ARN" \
    --type STANDARD >/dev/null
  echo "   created"
fi

echo "==> [3/3] Done"
echo "   arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:${SM_NAME}"
echo "   Run it:  aws stepfunctions start-execution --state-machine-arn arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:${SM_NAME}"
