# DeepF1 - Battery analysis sample using a Modern Data Architecture on AWS

## Architecture

Best practices applied during the design/development/deployment of such application are the following ones:<br />
- [Modern Data Analytics](https://aws.amazon.com/big-data/datalakes-and-analytics/modern-data-architecture/)<br />
- [The Twelve-Factor App](https://12factor.net/)
<br/><br/>

## Prerequisites
0 - To deploy it, **an AWS account is required** with the [AWS CLI installed and configured](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html) on your laptop.<br/>
AWS Region used for developments and tests: `eu-west-1`.<br/>

1 - If not already done, **do NOT forget** to install node on your laptop and the AWS CDK<br/>
Here is an example on OS X/MacOS:
```bash
brew install node
npm install -g aws-cdk
```

2 - **Docker Engine must be installed** on your laptop.
<br/><br/>

## Useful commands
Build & Deploy in your AWS environment:<br />
```bash
./cdk-deploy-to.sh [AWS_ACCOUNT_ID] [AWS_REGION] [AWS_PROFILE_NAME](optional)
```

To build your environment locally:<br />
```bash
npm run build
```

To check differences between your deployed environment and your local one:<br />
```bash
cdk diff
```

To delete your environment previously deployed:<br />
```bash
cdk destroy
```

## License
This application/project and its related code/assets/resources is licensed under the MIT-0 License. See the LICENSE file.