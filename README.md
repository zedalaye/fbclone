# fbclone
*Automatically exported from code.google.com/p/fbclone*

FBClone can clone a http://www.firebirdsql.org database in one shot (instead of backup/restore cycle) 
and pump data from one database to another with the same structure, it handles metadata / data charset 
conversion and may be useful to ease database owner change process or to migrate a database between 
two different firebird versions (eg. 2.1 -> 1.5)

Latest version have flags to ignore charset definitions from source database metadata so you can "normalize" 
a database with multiple charsets to only one, for instance, UTF8 (see -ics and -ko flags in conjunction to -tc UTF8)

In case fbclone saved your day and you want to pay me a beer, here's my Bitcoin address : 
1Nun2sC2Yj52GE8KrGpou7Vjkvi97ivj96
