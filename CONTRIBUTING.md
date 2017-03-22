# Welcome!
Thanks for taking an interest. We appreciate your help! The maintainers of this repo do so on a part-time basis, please be aware that it may take us a while to respond.

# Pull request guidelines
It'd be appreciated if you follow the recommendations below when submitting a patch:
* Add tests to verify your code. We use unit and integration tests extensively, see below. 
* We use [semantic versioning](https://docs.microsoft.com/en-us/nuget/create-packages/prerelease-packages#semantic-versioning
) and publish nuget packages directly out of the master branch. If your change is a breaking change or minor (backwards compatible) feature, please bump the minor or major version [here](https://github.com/lukeryannetnz/quartznet-dynamodb/blob/master/appveyor.yml#L10). If your change is just a bugfix, the build engine will bump the patch version for you.  

# Developer Guide
## Building locally

### Compiling
#### Windows/Visual Studio
If you're using windows/visual studio 2015 src/QuartzNET-DynamoDB.sln is what you're after. The solution file has nuget package restore on build so you should be good to go.

#### OSX/Linux
If you're using linux/osx this library compiles under mono. Our build infrastructure (travis-ci) uses a debian. Check out http://www.mono-project.com/. You'll want to install mono and make sure it's in your path as a minimum.
There are some shell scripts to help you with common tasks if you're that way inclined:

`./mono-nugetpackagerestore.sh` Restores nuget packages by invoking nuget.exe via mono

`./mono-compile.sh` Compiles the solution in release configuration using mono xbuild.

### Testing
The solution uses xunit tests extensively. These are broken into two groups:
#### Unit tests
These are small, target one class, where possible test state not behaviour and run entirely in memory. These are marked with the xunit trait Category=Unit.
#### Integration tests
These are more complex tests that test behavior. The initial integration tests were ported directly from the Quartz-NET RAMJobStoreTests. These require an instance of DynamoDB to test against. Amazon provide a java stand-alone version of dynamo, see [Running DynamoDB on your computer](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html). These are marked with the xunit trait Category=Integration.
