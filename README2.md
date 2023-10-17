# {ServiceName}

![Build Status]({CodeBuildBadgeUrl})

This project was generated using [Nx](https://nx.dev).

🔎 **Smart, Extensible Build Framework**

## Generate a data project

Run `npx nx g @domgen/nx-data:project <project>` to generate a data project.

> Full usage documentation <https://github.com/domgen/dgx-devops-nx-plugins/tree/develop/packages/nx-data#usage>

When using Nx, you can create multiple projects in the same workspace.

## Add Implicit Dependencies Between Projects

Data projects are not actually code projects, so there is no automatic relationship between them.

To set a dependency, add an `implicitDependencies` block to the project's `project.json` file.

```
{
  ...
  "implicitDependencies": ["<DependentProject>"]
}
```

Full documentation: [Nx Implicit Dependencies](https://nx.dev/latest/angular/core-concepts/configuration#implicit-dependencies)

## Build

Run `npx nx build my-app` to build the project. The build artifacts will be stored in the `dist/` directory.

## Understand your workspace

Run `npx nx dep-graph` to see a diagram of the dependencies of your projects.

## Resources

- [DynamoDB](https://domgen.atlassian.net/wiki/spaces/OPS/pages/2237136934/Data+Pipeline+-+DynamoDB)

## Further help

Visit the [Nx Documentation](https://nx.dev) to learn more.
