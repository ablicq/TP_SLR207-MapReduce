This project is a re-implementation of the map-reduce distributed algorithm in Java.

It is done as a part of the course *SLR207-Infrasctructures and platforms for distributed computing* at Télécom ParisTech.

# HOW TO:

the folder scripts contains several scripts to prepare and execute the mapreduce algorithm
	deploy.sh : deploy slave.jar to the hosts specified in slaves.conf
	clean_slaves.sh : clean the slaves folders (remove the splits, maps, and the slave.jar files)
	clean_master.sh : clean the master
	run_master.sh: run the master program
		takes two arguments: the file on which to count the words
			the size of the splits as specified in the [split man page](https://www.gnu.org/software/coreutils/manual/html_node/split-invocation.html)

the folder hadoop (in particular the folders hadoop/MASTER/src and hadoop/SLAVE/src) contains the source code for the project

the files slaves.conf contains the list of slaves to use (in the form user@host, one per line)

the folder rapport contains the markdown and pdf versions of the report demanded for the project
