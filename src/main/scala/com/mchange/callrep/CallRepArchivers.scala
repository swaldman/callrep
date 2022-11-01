// XXX: There's a lot of annoting completity here about managing 'dolt gc', without which
//      we produce too much garbage data, gigabytes of unnecessary storage. All the crap about starting
//      and killing the DBMS is so we can run 'dolt gc' safely. Once dolt integrates garbage collection
//      one way or another into its sql-server mode, all of that should be stripped away. Grrr. Usually
//      it's best to just have the DBMS running separately, and the archiver a client to it, rather than
//      managing the DBMS in the archiver, but the need for safe 'dolt gc' has us messing around.

package com.mchange.callrep

import java.io.{BufferedReader,File,PrintWriter,StringWriter};
import java.sql.{Connection,Statement};
import java.sql.Types._;
import java.text.SimpleDateFormat;
import java.util.{Calendar,Date};
import scala.collection._;
import scala.util.matching.Regex;
import com.mchange.sc.v2.io._
import com.mchange.sc.v2.lang._
import com.mchange.sc.v1.util.ClosableUtils._;
import com.mchange.sc.v1.sql.ResourceUtils._;
import com.mchange.sc.v1.log.MLevel._
import com.mchange.sc.v1.texttable
import com.mchange.sc.v1.superflex._;
import SuperFlexDbArchiver._

import scala.sys.process.Process

object CallRepArchivers
{
  private implicit val logger = mlogger( this )

  //override val schemaName = "callrep";

  //protected override val schemaDesc = "Bank call report data published by FFIEC";

  private val mbSchemaName           = None
  private val parentDirStr          = "datafiles/2022-10-29/FFIEC CDR Call"
  private val allSchedulesDirStr    = parentDirStr + "/FFIEC CDR Call Bulk All Schedules"
  private val subsetSchedulesDirStr = parentDirStr + "/FFIEC CDR Call Bulk Subset of Schedules"
  private val reportsDir            = "output/reports"

  private val dbmsDir = "output/dbms"
  private val dbDir   = dbmsDir + "/callrep"

  // recalling annoyingly that java.util.Calendar months are zero-indexed....
  private def toDate( year : Int, month : Int, day : Int ) = { val c = Calendar.getInstance(); c.clear(); c.set(year, month-1, day); c.getTime(); }

  private val downloadDate = toDate(2022,10,29)

  private val startYear = 2001;
  private val endYear   = 2022;

  private val DoltStartDelay = 1000;

  private val AllSchedulesDictionaryReportFile = new File( reportsDir, "AllSchedulesDictionary.txt" )
  private val BalShtIncStmtPastDueReportFile   = new File( reportsDir, "BalanceSheetIncomeStatementPastDueDictionary.txt" )

  object Dictionary {
    object Element {
      implicit val ElementOrdering = Ordering.by( (elem : Dictionary.Element ) => Element.unapply(elem).get )
    }
    final case class Element( field : String, label : String, tableName : String, typedecl : String )
  }
  class Dictionary {
    private val pages = mutable.SortedSet.empty[Dictionary.Element]

    def add( field : String, label : String, tableName : String, typedecl : String ) : Unit = this.synchronized {
      pages += Dictionary.Element( field, label, tableName, typedecl )
    }

    def add( element : Dictionary.Element ) : Unit = this.synchronized {
      pages += element
    }

    def addAll( elements : Iterable[Dictionary.Element] ) : Unit = this.synchronized {
      elements.foreach( this.add(_) )
    }

    def contents = this.synchronized {
      immutable.SortedSet.empty[Dictionary.Element] ++ pages
    }
  }

  case object DummyProcess extends Process {
    def destroy() : Unit = ()
    def exitValue : Int  = 0
  }

  // we want to run "dolt gc", and do so when the server is not running, after archiving each table
  // so we startup and stop the dbms
  def doltRestartServer() : Process = {
    val cmd = "dolt"::"sql-server"::Nil
    val wd  = new File( dbmsDir ) // sql-server should be run a level above database dirs
    val out = Process(cmd, wd).run()
    INFO.log("Restarting dolt sql-server.")
    Thread.sleep( DoltStartDelay ) // I'm not sure this is even necessary, but to be safe
    out
  }

  def doltGc() : Int = {
    val cmd = "dolt"::"gc"::Nil
    val wd  = new File( dbDir ) // gc should be run withing a database dir
    INFO.log("Running dolt gc.")
    val out = Process(cmd, wd).run().exitValue // wait for it to complete
    INFO.log("dolt gc completed.")
    out
  }

  val SchedulesDictionary = new Dictionary
  val BalShtIncStmtPastDueDictionary = new Dictionary

  protected abstract class CallRepArchiver( dictionary : Dictionary ) extends SuperFlexDbArchiver with TabDelimSplitter
  {
    private def dictionaryElements() : List[Dictionary.Element] = {
      val tableName = _unifiedTableInfo.get.tname.get
      val colInfos = _unifiedTableInfo.get.cols.get.toList
      colInfos.map( ci => Dictionary.Element(transformColName(ci.name),ci.label.getOrElse(""),tableName,ci.sqlTypeDecl.get) )
    }

    private def columnTable() : String = {
      val sw = new StringWriter()

      val colInfos = _unifiedTableInfo.get.cols.get.toList

      val tableCols = ("Name"::"Label"::"Type"::Nil).map(texttable.Column.apply)
      val tableRows = colInfos.map( texttable.Row(_) )

      texttable.appendTable( tableCols, (info : ColumnInfo) => (transformColName(info.name)::info.label.getOrElse("")::info.sqlTypeDecl.get::Nil) )( sw, tableRows )

      sw.toString
    }

    private def report() : String = {
      val df = new SimpleDateFormat("yyyy-MM-dd")
      val sw = new StringWriter()
      val pw = new PrintWriter(sw)
      pw.println(s"TABLE: ${_unifiedTableInfo.get.tableFullName.get}")
      pw.println()
      pw.println( "COLUMNS:")
      pw.println( columnTable() )
      pw.println()
      pw.println( "DATA SOURCES:" )
      _filesInfo.get.colNamesByFile.keys.foreach { ndfs =>
        pw.println( ndfs.sourceName )
      }
      pw.println()
      pw.println("PARSE ERRORS:")
      pw.println("      " + unreadableLinesXml( _fkdLineKeepers.get.values ))
      pw.println()
      // if (prologuesDefined()) {
      //   pw.println("MERGED PROLOGUES:")
      //   pw.println( _mergedPrologues.get )
      // }
      // pw.println()
      pw.println( "SOURCE: Financial Institutions Examination Council -- https://cdr.ffiec.gov/public/" );
      pw.println()
      pw.println(s"Data downloaded ${df.format(downloadDate)}, indexed and report generated ${df.format(new Date())}.")
      sw.toString()
    }

    private def saveReport() : Unit = {
      val dir = new File( reportsDir )
      dir.mkdirs()
      val f = new File( dir,_unifiedTableInfo.get.tableFullName.get + "-report.txt" )
      // println( s"Saving report to '${f}'." )
      f.replaceContents( report )
    }

    private def restartServer() : Process = {
      dbmsDialect match {
        case Dolt => doltRestartServer()
        case _    => DummyProcess
      }
    }

    private def cleanupServer( process : Process ) : Int = {
      if (process != DummyProcess ) {
        INFO.log("Waiting for DBMS server to terminate cleanly.")
        process.destroy()
        val out = process.exitValue() // wait for it to finish cleaning up before moving on!
        INFO.log("DBMS server terminated.")
        out
      }
      else 0
    }

    override def archiveFiles( csrc : ConnectionSource ) =
    {
      borrow( restartServer )( cleanupServer _ ) { _ =>
        super.archiveFiles( csrc );
      }
      saveReport()
      dictionary.addAll( this.dictionaryElements() )

      if (dbmsDialect == Dolt) doltGc()
    }

    override def archiveFilesNoDups( csrc : ConnectionSource, imposePkConstraint : Boolean ) =
    {
      borrow( restartServer )( _.destroy() ) { _ =>
        super.archiveFilesNoDups( csrc, imposePkConstraint );
      }

      saveReport()
      dictionary.addAll( this.dictionaryElements() )

      if (dbmsDialect == Dolt) doltGc()
    }

    // override val debugColumnInspection = true

    override val dbmsDialect = SuperFlexDbArchiver.Dolt

    override def transformColName( colName : String ) : String = 
    {
      val out = {
	if ( colName.length == 0 )
	  "unnamed";
	else
	  "\\W".r.replaceAllIn(colName,"_");
      }
      if (out != colName) {
        FINE.log(s"Column name: ${colName} -> ${out}")
      }
      out
    }

    override def isNull( datum : String, colName : String ) : Boolean =
      {
	if ( super.isNull( datum, colName ) )
	  true;
	else
	  {
	    colName match 
	    {
              case "Financial Institution State" => (datum == "0");
	      case _ => false;
	    }
	  }
      }

    override def readMetaData( br : BufferedReader ) : MetaData = 
      { 
	val colNameLine = br.readLine();
	val labelLine = br.readLine();
	val prologue = colNameLine + "\r\n" + labelLine + "\r\n";
	
	immutable.Map( Key.COL_NAMES -> split( colNameLine ).toList, Key.LABELS -> split( labelLine ).toList, Key.PROLOGUE -> (prologue::Nil) ); 
      };
  }

  private class BsispdArchiver(filenum : Int) extends {

    val priorTableInfo = TableInfo( mbSchemaName, Some("BalanceSheetIncomeStatementPastDue%d".format(filenum)), None, Some("IDRSSD"::"Reporting Period End Date"::Nil) );

  } with CallRepArchiver(BalShtIncStmtPastDueDictionary) {

    val files = {
      val sfxDirTemplate = subsetSchedulesDirStr + "/FFIEC CDR Call Bulk Subset of Schedules %d"
      var sfxTemplate = sfxDirTemplate + "/FFIEC CDR Call Subset of Schedules %d(%d of %d).txt";
      //def numFilesForYear( yrNum : Int ) = if (yrNum > 2010) 3; else 2; //hard coding number of files per year!
      //val startYear = if (filenum <= 2) 2001; else 2011;

      def numFilesForYear( yrNum : Int ) = {
        val dir = new File( sfxDirTemplate.format(yrNum) )

        // println(dir)
        // println(dir.exists)


        if (dir.exists) {
          dir.list.filter( _.startsWith("FFIEC CDR Call Subset of Schedules") ).length
        }
        else {
          0
        }
      }

      var fileNames = (startYear to endYear).map( n => sfxTemplate.format( n, n, filenum, numFilesForYear(n) ) );
      // fileNames.filter( new File( _ ).exists() ).foreach( println _ );
      fileNames.filter( new File( _ ).exists() ).map( FileDataFileSource( _ ) ).toSeq;
    };

    // val descr : String =
    //   ("Selected call report fields, the %s of three tables published by FFIEC " +
    //    "under the heading 'Balance Sheet, Income Statement, and Past Due, 4 Periods'").format( List("zeroth","first","second","third")(filenum) );
    
    // val attribution : String = "Federal Financial Institutions Examination Council";
    // val src_url     : String = "https://cdr.ffiec.gov/public/";
    // val downloaded  : Date   = toDate(2011,9,20);

    protected override val bufferSize = 300 * 1024 * 1024; //300M

  }

  private def extractSinglePeriodCallReportFileInfo( parentDirName : String ) : Map[String, Map[NamedDataFileSource, String]] = //tableName -> Map of (fully qualified datasources -> date part of name)
  {
    //var dirRegex = """FFIEC-\d{4}-Q\d""".r; //a directory containg one quarter's report's

    var dirRegex = """FFIEC CDR Call Bulk All Schedules \d{8}""".r; //a directory containg one quarter's reports
    var fileRegexTemplate = """FFIEC CDR Call (Schedule|Bulk) (%s) (\d{8})(?:\((%s) of \d\))?.txt""";
    var anyFileRegex = fileRegexTemplate.format("""\w+""","""\d""").r;
    def scheduleFileRegexAnyNum( scheduleName : String ) = fileRegexTemplate.format( scheduleName, """\d""" ).r; //I don't know why overloading seems to fail here
    def scheduleFileRegex( scheduleName : String, num : Int ) = fileRegexTemplate.format( scheduleName, num.toString() ).r;
    
    val parentDir = new File( allSchedulesDirStr );

    // println(parentDir)
    // println(parentDir.exists)
    // println(parentDir.list.mkString("\n"))

    val allDirs = immutable.Set( parentDir.list().filter( dirRegex.findPrefixMatchOf( _ ) != None ).map( new File( parentDir, _ ) ) : _* ) ;
    
    val allNames = mutable.Set.empty[String];
    val numberedNames = mutable.Map.empty[String, mutable.Set[Int]];
    
    for( dir <- allDirs; fname <- dir.list(); maybeMatch = anyFileRegex.findPrefixMatchOf( fname ); if (maybeMatch != None))
      {
	var m = maybeMatch.get;

	var sname = m.group(2);
	allNames += sname;

	// test for number suffix
	val numStr = m.group(4);
	if (numStr != null) //file has a "(1 of 2)"-style bit
	  {
	    // a concise setup for lazy caching... note that the second param of getOrElse is "by name", so newSet is evaluated only if necessary
	    def newSet = { val mt = mutable.Set.empty[Int]; numberedNames += (sname -> mt); mt};
	    val intSet : mutable.Set[Int] = numberedNames.getOrElse( sname, newSet );
	    intSet += numStr.toInt;
	  }
      }

    val noNumNames = allNames.filter( numberedNames.get( _ ) == None );

    def _mapForName( scheduleName : String, regex : Regex, schedType : String ) : Map[NamedDataFileSource,String] =
      {
	val out = mutable.Map.empty[NamedDataFileSource,String];
	for ( dir <- allDirs)
	  {
	    // println( dir );
	    val dirTups = for (fname <- dir.list(); maybeMatch = regex.findPrefixMatchOf( fname ); if (maybeMatch != None); m = maybeMatch.get) 
			    yield Tuple2( FileDataFileSource( new File( new File(parentDir, dir.getName()), fname ) ), m.group(3) );

	    // dirTups.foreach( println _ );

	    val goodTup =
	      dirTups.length match
	      {
		case 0 => { printf("No %s with name '%s' found in '%s'. Skipping.\n", schedType, scheduleName, dir.getName()); null }
		case 1 => dirTups(0);
		case l @ _ => { throw new RuntimeException("More than one (%d) %s with name '%s' found in '%s'.".format( l, schedType, scheduleName, dir.getName()) ); }
	      };
	    if (goodTup != null)
	      out += goodTup;
	  }
	immutable.Map.empty ++ out;
      }

    def mapForNoNumName( scheduleName : String ) : Map[NamedDataFileSource,String] =
      { _mapForName( scheduleName, scheduleFileRegexAnyNum( scheduleName ), "non-numbered schedule" ); }

    def mapForNumName( scheduleName : String, num : Int ) : Map[NamedDataFileSource,String] =
      { _mapForName( scheduleName, scheduleFileRegex( scheduleName, num ), "schedule with number " + num ); }

    val out = mutable.Map.empty[String, Map[NamedDataFileSource, String]];
    for( name <- noNumNames)
      out += ( name -> mapForNoNumName( name ) );
    for ( name <- numberedNames.keys; num <- numberedNames(name) )
      out += ( name + num -> mapForNumName( name, num ) );

    immutable.Map.empty ++ out;
  }

  /**
   *  This is particularly annoying, because the "Reporting Period End Date", part of the primary key of the combined table
   *  is not actually included in the data. Note that in readMetaData, we artificially add a header not in the data,
   *  and we define a transformDataLine() method that appends the date encoded in the filename into the file.
   *
   *  Also, one header, IDRSSD, is bizarrely quoted. So, we have to override our standard tab delim trim function.
   *  to unquote. Grrr.
   *
   *  This is ridiculously annoying. I don't envy you, dear reader.
   */ 
  private class BySinglePeriodCallReportArchiver(tableName : String, fileDateStrMap : Map[NamedDataFileSource,String]) extends {
    val dateHeader = "Reporting Period End Date";
    val priorTableInfo = TableInfo( mbSchemaName, Some(tableName), None, Some("IDRSSD"::dateHeader::Nil) );
  } with CallRepArchiver(SchedulesDictionary) {
    
    override def readMetaData( br : BufferedReader ) : MetaData = 
      { 
	val colNameLine = br.readLine();
	val labelLine = if (tableName != "POR") { br.readLine(); }; else { colNameLine }; // unlike all other tables the POR report has no label line
	val prologue = colNameLine + "\r\n" + labelLine + "\r\n";
	
	immutable.Map(Key.COL_NAMES -> (split( colNameLine ).toList:::dateHeader::Nil), 
		      Key.LABELS -> (split( labelLine ).toList:::dateHeader::Nil), 
		      Key.PROLOGUE -> (prologue::Nil) ); 
      };

    override def split(row : String) : Array[String] = 
      { 
	val sup = super.split( row );
	sup.map //undo quotes...
	{
	  s =>
	    if (s.length > 1 && s(0) == '"' && s(s.length - 1) == '"')
	      s.substring(1 /* from inclusive */, s.length-1 /*to exclusive */);
	    else
	      s;
	}
      };

    override def prepareTransformFileData( f : NamedDataFileSource, colNames : Array[String] ) : Tuple2[Any,Any] = Tuple2( super.prepareTransformFileData( f, colNames ), fileDateStrMap(f) );

    override def transformUnsplitDataLine( line : String, prepInfo : Tuple2[Any,Any] ) : String = ( super.transformUnsplitDataLine(line, prepInfo._1.asInstanceOf[Tuple2[Any,Any]]) + "\t" + prepInfo._2 );

    override def transformSplitData( data : Array[String], prepInfo : Tuple2[Any,Any] ) : Array[String] = super.transformSplitData(data, prepInfo._1.asInstanceOf[Tuple2[Any,Any]]);

    override val files = fileDateStrMap.map( _._1 ).toSeq;

    // override val descr : String = "Selected call report fields, published by FFIEC as 'SinglePeriodCallReports' as '" + tableName + "'";

    override val strict = false;

    override val bufferSize = 50 * 1024 * 1024; //50M, smallish, cuz we read many files in parallel here
    
    // override val attribution : String = "Federal Financial Institutions Examination Council";
    // override val src_url     : String = "https://cdr.ffiec.gov/public/";
    // override val downloaded  : Date   = downloadDate;
  }

  private val schedulesInfo : Map[String, Map[NamedDataFileSource, String]] = extractSinglePeriodCallReportFileInfo( allSchedulesDirStr );

  private def archiveBsispd( csrc : ConnectionSource ) =
  {
    val archiver1 = new BsispdArchiver(1);
    val archiver2 = new BsispdArchiver(2);
    val archiver3 = new BsispdArchiver(3);
    
    archiver1.archiveFilesNoDups( csrc, true );
    archiver2.archiveFilesNoDups( csrc, true );

    try {
      archiver3.archiveFilesNoDups( csrc, true );
    }
    catch {
      case ute : UndefinedTableException => INFO.log("Skipping third BSISPD table, data not found.")
    }
  }

  private def archiveSinglePeriodCallReports( csrc : ConnectionSource ) =
  {
    schedulesInfo.foreach
    {
      tup =>
	{
	  val archiver = new BySinglePeriodCallReportArchiver( tup._1, tup._2 );
	  archiver.archiveFilesNoDups( csrc, true );
	}
    }
  }

  private def dictionaryReport( dictionary : Dictionary ) : String = {
    val sw = new StringWriter()

    val elements = dictionary.contents

    val tableCols = ("Name"::"Label"::"Table"::"Type"::Nil).map(texttable.Column.apply)
    val tableRows = elements.toList.map( texttable.Row(_) )

    texttable.appendProductTable( tableCols )( sw, tableRows )

    sw.toString
  }

  def archiveTables( csrc : ConnectionSource ) = {
    archiveBsispd( csrc )
    BalShtIncStmtPastDueReportFile.replaceContents( dictionaryReport( BalShtIncStmtPastDueDictionary ) )

    archiveSinglePeriodCallReports( csrc )
    AllSchedulesDictionaryReportFile.replaceContents( dictionaryReport( SchedulesDictionary ) )
  }


  // this code expects the "callrep" db to already exist.
  // so the server needs to have been started using dbmsDir as its data directory, and the db callrep (re)created there
  def main( argv : Array[String] ) : Unit = {
    import java.sql._
    import com.mchange.callrep._
    import com.mchange.v2.log._

    MLog.forceFallback( MLevel.FINEST )
    val connectionSource = new Object {  def getConnection() : Connection = DriverManager.getConnection("jdbc:mysql://localhost/callrep", "root", "") }
    //val connectionSource = new Object {  def getConnection() : Connection = DriverManager.getConnection("jdbc:postgresql://localhost/callrep", "swaldman", "") }
    archiveTables( connectionSource )
  }
}
