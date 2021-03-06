## Run Spline examples 
To run Spline example, download the Spline source code from GitHub and switch to the `examples` directory.     
```shell script
cd $SPLINE_PROJECT/examples
```

To run all available examples:
```shell script
mvn test -P examples
```

To run a selected example job (e.g. `Example1Job`):
```shell script
mvn test -P examples -D exampleClass=za.co.absa.spline.example.batch.Example1Job
``` 

The different examples are located here:

[batch](https://github.com/AbsaOSS/spline/tree/develop/examples/src/main/scala/za/co/absa/spline/example/batch)
[batchWithDependencies](https://github.com/AbsaOSS/spline/tree/develop/examples/src/main/scala/za/co/absa/spline/example/batchWithDependencies)

To change the Spline Producer URL (default is http://localhost:8080/producer):
```shell script
mvn test -P examples -D spline.producer.url=http://localhost:8888/producer
```


 

---

    Copyright 2019 ABSA Group Limited
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
