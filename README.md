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
will as well, if it can just be cloned as an interactively queriable database.

### Call Reports on DoltHub

The database this code generates is now [pubished on dolthub](https://www.dolthub.com/repositories/swaldman/callrep). You should just [install](https://docs.dolthub.com/introduction/installation) `dolt` and clone the database!

```
$ dolt clone swaldman/callrep
...
...
...
0 of 2,613,690 chunks complete. 786,432 chunks being downloaded currently.
Downloading file: 6751bq0l28a7qfv5eib7r8kibvbl5dju (262,144 chunks) - 8.10% downloaded, 450 kB/s
0 of 2,613,690 chunks complete. 786,432 chunks being downloaded currently.
Downloading file: 6751bq0l28a7qfv5eib7r8kibvbl5dju (262,144 chunks) - 8.12% downloaded, 446 kB/s
0 of 2,613,690 chunks complete. 786,432 chunks being downloaded currently.
Downloading file: 6751bq0l28a7qfv5eib7r8kibvbl5dju (262,144 chunks) - 8.12% downloaded, 446 kB/s
$ cd callrep/
$ dolt sql -q "SELECT YEAR(Reporting_Period_End_Date) AS YEAR, COUNT(*) AS NUM_FILERS FROM BalanceSheetIncomeStatementPastDue1 WHERE MONTH(Reporting_Period_End_Date) = 12 GROUP BY YEAR ORDER BY YEAR ASC;"
+------+------------+
| YEAR | NUM_FILERS |
+------+------------+
| 2001 | 8689       |
| 2002 | 8468       |
| 2003 | 8348       |
| 2004 | 8179       |
| 2005 | 8056       |
| 2006 | 7922       |
| 2007 | 7788       |
| 2008 | 7568       |
| 2009 | 7321       |
| 2010 | 6999       |
| 2011 | 6789       |
| 2012 | 7150       |
| 2013 | 6877       |
| 2014 | 6570       |
| 2015 | 6238       |
| 2016 | 5966       |
| 2017 | 5721       |
| 2018 | 5456       |
| 2019 | 5227       |
| 2021 | 4887       |
+------+------------+
```

You can find reports documenting all available tables [here](https://www.mchange.com/projects/callrep/2022-10-29/).

That's kind of a mess, but here's what you really need:

* Information about the most useful single table, [BalanceSheetIncomeStatementPastDue1](https://www.mchange.com/projects/callrep/2022-10-29/BalanceSheetIncomeStatementPastDue1-report.txt)
* A [dictionary](https://www.mchange.com/projects/callrep/2022-10-29/BalanceSheetIncomeStatementPastDueDictionary.txt) of where you find data across the three `BalanceSheetIncomeStatementPastDue` tables
* A [dictionary](https://www.mchange.com/projects/callrep/2022-10-29/AllSchedulesDictionary.txt) of where you find data across all of the individual schedule tables (`POR`,`CI`, `RCO1`, etc.)

---

_**Note:** Several of the individual schedules (but not the schedules BalanceSheetIncomeStatementPastDue tables) reported parse errors on while archiving.
If you are querying against tables_

* [`NARR`](https://www.mchange.com/projects/callrep/2022-10-29/NARR-report.txt)
* [`RCF`](https://www.mchange.com/projects/callrep/2022-10-29/RCF-report.txt)
* [`RCQ2`](https://www.mchange.com/projects/callrep/2022-10-29/RCQ2-report.txt)
* [`RIE`](https://www.mchange.com/projects/callrep/2022-10-29/RIE-report.txt)

_please scroll to the bottom of the linked reports for information about the lines in those schedules that could not be parsed and were skipped._

### Building the database yourself

If you are masochistic, you can build the callrep database yourself. Datafiles are untowardly included in this (gigantic) git
repository. If you pull, you'll get the call report datafiles as of 2022-10-29.

#### Prerequisites

1. The `dolt` binary must be installed and present on the `PATH` when you run this archiver.
2. The archiver needs to be run under Scala 2.11, on a Java 8 JVM. The duck-typing it uses fails with `SecurityException`s on newer JVMs.
3. You will need the [build tool `sbt`](https://www.scala-sbt.org/) in your PATH as well.
4. This application is a memory hog. I don't know what's strictly necessary, but the archiver is configured to run with up to 25GB of heap space.


#### Note: The archiver manages `dolt`!

Normally my database archivers expect a DBMS up and running, then you just run the archiver which communicates with
the database via JDBC. However, we are now archiving this to [dolt](https://www.dolthub.com/), which generates a very
large amount of temporary storage while archiving, whch must be cleaned up by calling `dolt gc` while the DBMS server
is not running. So currently, this script manages the starting, stopping, and garbage collection of `dolt` by itself.

  **TL; DR:** You must make sure that **no `dolt` server is running when you execute this archiver**, so that the archiver
can manage (start, stop, restart) its own.

#### Step by step

1. Make sure that `dolt` and a java 8 VM are on your path:
   ```
   $ java --version
   java 11.0.3 2019-04-16 LTS
   Java(TM) SE Runtime Environment 18.9 (build 11.0.3+12-LTS)
   Java HotSpot(TM) 64-Bit Server VM 18.9 (build 11.0.3+12-LTS, mixed mode)
   $ dolt version
   dolt version 0.50.9
   database storage format: NEW ( __DOLT__ )

   ```
2. Prepare your repository with
   ```
   $ ./reset-output.sh
   ```
   This will create an empty `dolt` database and directories for both the output database and reports.
3. Enter the `sbt` command line and type `run`.
   ```
   sbt
   [info] welcome to sbt 1.7.2 (Oracle Corporation Java 11.0.3)
   [info] loading settings for project global-plugins from dependency-graph.sbt,gpg.sbt,metals.sbt ...
   [info] loading global plugins from /Users/swaldman/.sbt/1.0/plugins
   [info] loading project definition from /Users/swaldman/Dropbox/BaseFolders/development-why/gitproj/callrep-archiver/project
   [info] loading settings for project root from build.sbt ...
   [info] set current project to callrep (in build file:/Users/swaldman/Dropbox/BaseFolders/development-why/gitproj/callrep-archiver/)
   [info] sbt server started at local:///Users/swaldman/.sbt/1.0/server/b150fc96f6f1c0c01761/sock
   [info] started sbt server
   sbt:callrep> run
   
   ```
   It'll probably take overnight (or more) to archive complete database. You'll see lots of crap scroll by while it does.
4. [Commit](https://docs.dolthub.com/concepts/dolt/git/commits) the archived database, and try a query:
   ```
   $ cd output/dbms/callrep
   $ dolt add .
   $ dolt commit -m "Initial import"
   $ dolt sql -q "SELECT Financial_Institution_Name, RCFD2170 AS TOTAL_ASSETS FROM BalanceSheetIncomeStatementPastDue1 WHERE Reporting_Period_End_Date = '2022-06-30' ORDER BY TOTAL_ASSETS DESC LIMIT 30;"
   +---------------------------------------------------+--------------+
   | Financial_Institution_Name                        | TOTAL_ASSETS |
   +---------------------------------------------------+--------------+
   | JPMORGAN CHASE BANK, NATIONAL ASSOCIATION         | 3380824000   |
   | BANK OF AMERICA, NATIONAL ASSOCIATION             | 2440022000   |
   | CITIBANK, N.A.                                    | 1720308000   |
   | WELLS FARGO BANK, NATIONAL ASSOCIATION            | 1712535000   |
   | U.S. BANK NATIONAL ASSOCIATION                    | 582252757    |
   | PNC BANK, NATIONAL ASSOCIATION                    | 534346587    |
   | TRUIST BANK                                       | 532080000    |
   | GOLDMAN SACHS BANK USA                            | 501906000    |
   | CHARLES SCHWAB BANK, SSB                          | 407901000    |
   | TD BANK, N.A.                                     | 405223010    |
   | CAPITAL ONE, NATIONAL ASSOCIATION                 | 388439751    |
   | BANK OF NEW YORK MELLON, THE                      | 365102000    |
   | STATE STREET BANK AND TRUST COMPANY               | 296434000    |
   | CITIZENS BANK, NATIONAL ASSOCIATION               | 226531535    |
   | SILICON VALLEY BANK                               | 211824000    |
   | FIFTH THIRD BANK, NATIONAL ASSOCIATION            | 205546136    |
   | MANUFACTURERS AND TRADERS TRUST COMPANY           | 203656265    |
   | MORGAN STANLEY PRIVATE BANK, NATIONAL ASSOCIATION | 199887000    |
   | FIRST REPUBLIC BANK                               | 197908327    |
   | MORGAN STANLEY BANK, N.A.                         | 191345000    |
   | KEYBANK NATIONAL ASSOCIATION                      | 184673175    |
   | HUNTINGTON NATIONAL BANK, THE                     | 178091290    |
   | ALLY BANK                                         | 175814000    |
   | HSBC BANK USA, NATIONAL ASSOCIATION               | 168924907    |
   | BMO HARRIS BANK NATIONAL ASSOCIATION              | 163203086    |
   | REGIONS BANK                                      | 159787000    |
   | NORTHERN TRUST COMPANY, THE                       | 157289965    |
   | AMERICAN EXPRESS NATIONAL BANK                    | 137922091    |
   | CAPITAL ONE BANK (USA), NATIONAL ASSOCIATION      | 126717894    |
   | MUFG UNION BANK, NATIONAL ASSOCIATION             | 124662227    |
   +---------------------------------------------------+--------------+
   ```

That's it!

