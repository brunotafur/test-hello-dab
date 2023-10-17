# dgx-template-data

Template for a Monorepo data Project

## Details

This template uses the [@domgen/nx-data](https://github.com/domgen/dgx-devops-nx-plugins/tree/develop/packages/nx-data) to generate the projects with the correct format and also uses the [@domgen/nx-aws-cache](https://github.com/domgen/dgx-devops-nx-plugins/tree/develop/packages/nx-aws-cache) to provide the remote caching feature using AWS S3 Bucket.

## Usage

1. Edit `package.json` and update `name`, `description` and `repository.url`.
2. Edit `nx.json` and update `npmScope`.
3. Edit `README2.md`:
   1. Replace all occurences of `{ServiceName}` with the actual service name.
   2. Replace `{CodeBuildBadgeUrl}` with the CodeBuild build badge URL, changing the `branch` parameter from `master` to `develop`.
4. Delete `README.md` and rename `README2.md` to `README.md`

###### Notes

_CodeBuild Image: aws/codebuild/standard:5.0-22.06.30_
