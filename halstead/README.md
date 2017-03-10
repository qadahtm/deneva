# Running Deneva on Halstead cluster

## Requesting Resources from Halstead PBS
One way is to run Deneve on Halstead in interactibve mode. This allows to obtain exlusive
access to nodes and perform experiments interactively. 

Another way is t submit a script to run the experiment, which requires writing the script.
For now, we will just focus on running experiments in interacrtive mode. 

To request resources from Halstead, use ssh and your Career credintials to login to the 
cluser. On the front-end machine, run the following command to request 2 nodes with all 
of their cores and memory resources. 

```ssh $ qsub -I -l 
nodes=2:ppn=20:A,walltime=02:00:00,naccesspolicy=singleuser 
```

The above command will request 2 nodes with 20 cores, and you will have these 2 nodes 
for 2 hours. The request will go to the `standby` queue which is shared among all 
cluster users.


## Building Deneva
Note: you will need to request a node for building the code as it seems agains the user 
policy of Halstead to run intensive tasks on the front-end.

The code depends on few libraries:
- Boost (atomic, lockfree ...)
- nanomsg
- jemalloc

I am using gcc-5.2.0 for building this. 
On Halstead this can be loaded using:

`$ module load gcc/5.2.0`

You also need to create an obj directory. Perhaps we can modify the Makefile to do this for us implicitly.


### Building dependencies
Dependencies are included in the `deps` directory of the repo.  
#### Boost
TBD
#### nanomsg
TBD
### jemalloc


## Running Deneva
Refer to the README.md at the root directory. I will add details specific to halstead 
here if any
