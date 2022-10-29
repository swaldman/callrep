# callrep archiver

### Introduction

This is insanely embarrassing code. It is based on a project I did in 2009, as a java programmer
first beginning to play with scala. It build on top of the also insanely embarrassing (but somehow
very powerful) [superflex](https://github.com/swaldman/superflex) project, written at the same time and with the same awkward
naivity to sane scala programming.

Nevertheless, it does very usefully convert [FFIEC's miserable-to-work-with bank call report data](https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx) into
a usable SQL database. The data format seems not to have very much changed, thank goodness.

My revisiting of this ancient embarrassment is inspired by [dolt](https://github.com/dolthub/dolt), which promises to make nicely digested SQL databases
sharable, rather than merely textfiles. When I used to keep this database around, I found it useful to play with. Maybe others
will as well, if it can just be closed as an interactively queriable database.

### Tips

* This needs to be run under Scala 2.11, on a Java 8 JVM. The duck-typing it uses fails with `SecurityException`s on newer JVMs.

