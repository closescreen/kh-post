import std.stdio, std.string, std.getopt, core.stdc.stdlib, std.regex, std.algorithm, std.concurrency, requests;
import std.process, std.functional, std.conv, core.thread;

void main(string[] args) {
  auto synopsis = "Communicates with ClickHouse server via HTTP POST.\nUsage: echo abc | khpost options...";
  auto samples = "
# explicity definition '--server' option:
khpost -s\"kh1\" -q'show tables from rnd600' 

# 'server' option as environment variable:
export kh_server=kh1.myorg

t=\"rnd600.test3\" # --bash variable

khpost -q\"CREATE TABLE IF NOT EXISTS $t ( sid UInt64,  name String,  d Date DEFAULT today()) ENGINE = MergeTree(d, sid, 8192)\"
#OR: echo \"CREATE TABLE IF NOT EXISTS $t ( sid UInt64,  name String,  d Date DEFAULT today()) ENGINE = MergeTree(d, sid, 8192)\" | khpost -i

khpost -q\"show create table $t\"
#OR: echo \"show create table $t\" | khpost -i

echo -e \"1\\t'site1'\" | khpost -i -q\"insert into $t (sid,name)\" -ftsv

# Using '-y' for testing non-zero query result (without using 'grep'):
khpost -y\"exists table mydb.mytable\" || echo NO

# -q will exit with status(1) if http return code != 200:
khpost -q\"create table test_wrong_definition\" || echo \"NO\"

# Simple queryes may by pass without '-q':
khpost exists mytable # <-- will treated as: \"khpost -q'exists mytable'\"

# Using alias '~':

kh_database=rnd600 khpost \"show tables from ~\"
#or:
khpost -~rnd600 \"show tables from ~\" # - the same

";
  auto server = environment.get("kh_server", "");
  auto tilda_alias = environment.get("kh_database","");
  string query;
  auto read_stdin = false;
  auto yes_query = "";
  auto yes_re_str_default = `[^0\s]`;
  auto yes_re_str = "";
  auto int_query = "";
  auto proto = "http://";
  auto port = "8123";
  auto chunk_size = 100_000_000;
  auto content_type = "application/binary";	
  auto deb = 0;
  auto timeout = 0; // seconds
  auto verbosity = 0;
  bool errors= false;
  auto expect = [200];
  auto format = "";
  auto danger_admited = false;
  auto formats = [
    "tsv": "TSV", "tsvn": "TSVWithNames", "tsvnt": "TSVWithNamesAndTypes", "tsvr": "TSVRaw", "btsv": "BlockTabSeparated",
    "csv": "CSV", "csvn": "CSVWithNames",
    "rb": "RowBinary",
    "p": "Pretty", "pc": "PrettyCompact", "pcmb": "PrettyCompactMonoBlock", "ps": "PrettySpace", 
    "pne": "PrettyNoEscapes", "pcne": "PrettyCompactNoEscapes", "psne": "PrettySpaceNoEscapes",
    "v":"Vertical",
    "vv":"Values",
    "j":"JSON", "jc": "JSONCompact", "jer":"JSONEachRow",
    "tskv":"TSKV",
    "xml":"XML",
    ];
  
  try{
    auto cli = getopt( args,
	"server|s", "server address f.e. kh1.myorg. May be used environment variable 'kh_server' [%s].".format(server), &server,
	"query|q", "string for pass to server as query.", &query,
	"tilda|~", "the string to be replace '~' which in queryes '-q|-y' (stdin not processed). Usefull as alias for database name.", &tilda_alias,
	std.getopt.config.caseSensitive,
	"FORCE", "if text in '-q|-y' has /drop|alter/i, then you must enable this flag (stdin will not checked)", &danger_admited,
	std.getopt.config.caseInsensitive,
	"stdin|i", "read first part of query from '-q', and second part from STDIN.", &read_stdin,
	"yes|y", "--yes=<sql> executes <sql> (after -q) and exit with 0 if result match by m/[^0\\s]/, and 3 otherwise", &yes_query,
	"regex|r", "match '--yes' result with given regex. exit(0) if matched, exit(3) otherwise", &yes_re_str, 
	"int", "Print answer for this query as long integer. Print 0 if answer is empty or has not digits.", &int_query,
	"format|f", "phrase: ' FORMAT <format>' will added to --query string", &format,
	"content-type", "content-type header for input data [%s]".format(content_type), &content_type,
	"chunk-size", "chunk size in bytes [%s]".format(chunk_size), &chunk_size,
	"timeout|t", "number of seconds to wait server answer [%s]".format(timeout), &timeout, 
	"deb|d+", "increase debug messages level. Multiple '-d' allowed.", &deb,
	"verbosity|v+", "increment verbosity level for http requests. Multiple '-v' allowed to increase it.", &verbosity,
	"errors|e", "print to stderr error message if '--yes' fail.", &errors,
	"expect", "expected http codes. List of codes. %s".format(expect), &expect,
	"port","server port [%s]".format(port), &port,
	"proto","server protocol [%s]".format(proto), &proto,
    );
    if( cli.helpWanted ) 
        defaultGetoptPrinter( synopsis, cli.options), 
        writefln("Format in --format maybe:\n%s \nSample:%s", formats, samples), exit(0);
  }
  catch(Throwable o) stderr.writeln(o.msg), exit(1);

  if( empty( server)) stderr.writeln("khpost: server not defined!"), exit(1);
  else if( !matchFirst( server, regex("https?://"))) server = proto ~ server; // 'kh.myorg' --> 'http://kh.myorg'
  !matchFirst( server, regex(`:\d+$`)) && ( server ~= ":" ~ port ); // 'http://kh.myorg' --> 'http://kh.myorg:8123'
  
  if ( query.empty && args.length ) query = args[1..$].join(" "); // if empty '-q' other args became '-q'
  if (!read_stdin && query.empty && yes_query.empty && int_query.empty) stderr.writeln("Either -i or -q or -y or -n must be defined."), exit(1);

  if ( yes_re_str.not!empty && yes_query.empty ) stderr.writeln("--regex=<re> without --yes=<sql> not allowed."), exit(1);
  if ( yes_re_str.empty ) yes_re_str = yes_re_str_default;

  deb && stderr.writefln( 
    "deb:%s, verb:%s, tmout:%s server: %s, expect_codes: %s, chunk_size: %s, content_type: %s, ~:%s, -q:%s, -y:%s",
    deb,   verbosity, timeout, server,     expect,           chunk_size,     content_type, tilda_alias ,query, yes_query
  );

  auto timer = 0;
  
  if ( tilda_alias.not!empty ){
      query = query.replaceAll( regex("~"), tilda_alias);
      yes_query = yes_query.replaceAll( regex("~"), tilda_alias);
      int_query = int_query.replaceAll( regex("~"), tilda_alias);
      deb && stderr.writefln("'~' replaced with '%s'.", tilda_alias);
  }

  foreach ( text; [query , yes_query, int_query] ){
    if ( matchFirst( text, regex(`drop|alter`, "i")) && !danger_admited ){ 
      timer = 15;
      stderr.writefln("-----------------------------------------------------------\n" ~
      "Dander query found. And may be executed:\n%s\nCtrl+C to abort (wait %s sec). Use --FORCE to disable timer.\n", text, timer);
      break;
    }   
  }
  
  if (timer)
    foreach (i; 1..timer+1 ){ 
      Thread.sleep( 1.seconds );
      stderr.writef("%s ", i);
    }
  
  
  auto fin = stdin;
  auto fout = stdout;
  auto ferr = stderr;
  
  if ( query.not!empty || read_stdin ){
    deb>1 && ferr.writeln("spawn child process");
    auto childTid = spawn( &childPostSender, deb, verbosity, timeout, server, content_type );

    bool myblock(Tid tid){ deb>1 && ferr.writeln("blocking on mailbox maxsize (sending slower then reading)"); return false; }  
  
    setMaxMailboxSize( childTid, 10, OnCrowding.block);
  
    if (format.not!empty) format = formats.get( format.toLower, format );
    
    deb && ferr.writefln( "Sending query: %s ...", query );
    childTid.send( ( query.chomp ~ ( format.not!empty ? " FORMAT "~format : "" ) ~ "\n" ).representation );
  
    if ( read_stdin ){
      deb && ferr.writeln("read stdin for send to child...");

      if (deb>2) foreach( chunk; fin.byChunk( chunk_size) ){
        ferr.writefln("send: %s", chunk); // full text
        childTid.send( chunk.idup);
      }
      else if (deb>1) foreach( chunk; fin.byChunk( chunk_size) ){
        ferr.write("~"); // like a progress bar >>>>>
        childTid.send( chunk.idup);
      }
      else foreach( chunk; fin.byChunk( chunk_size) ){ // withiout bedug
        childTid.send( chunk.idup);
      }


    }    
    childTid.send(true);// that's all
    auto answer_code = receiveOnly!int();
    if( !expect.canFind( answer_code)){
      deb && ferr.writefln("Answer code: %s", answer_code);
      exit(1);
    }
  }  
  
  if ( yes_query.not!empty ){
      auto rq = Request();
      rq.timeout = timeout.seconds;
      if (verbosity) rq.verbosity = verbosity;
      auto rs = rq.post( server, yes_query, content_type);
      if( !expect.canFind( rs.code)){
        ferr.writefln("Response code: %s\n--yes=%s.\n%s Stop executing.", rs.code, yes_query, rs.responseBody );
        exit(1);
      }
      if ( rs.responseBody.to!string.matchFirst( yes_re_str )){ 
        deb && ferr.writefln("Response body: \n%s\n after --yes=\"%s\" contain smth 'yes' symbols.", rs.responseBody, yes_query);
        exit(0);
      }else{
        deb && ferr.writefln("Response body: \n%s\n after --yes=\"%s\" not contain any 'yes' symbols.", rs.responseBody, yes_query);
        errors && ferr.writefln("\"%s\" - returns NO.", yes_query);
        exit(3);
      }
  }

  if ( int_query.not!empty ){
      auto rq = Request();
      rq.timeout = timeout.seconds;
      if (verbosity) rq.verbosity = verbosity;
      auto rs = rq.post( server, int_query, content_type);
      if( !expect.canFind( rs.code)){
        ferr.writefln("Response code: %s\n--num=%s.\n%s Stop executing.", rs.code, int_query, rs.responseBody );
        exit(1);
      }
      long rv = 0;
      if ( auto m = rs.responseBody.to!string.matchFirst( regex(r"-?\d+")) ){
//      if ( rs.responseBody.to!string.matchFirst( yes_re_str )){ 
        deb && ferr.writefln("Response body: '%s' after --num=\"%s\" contain integer: %s.", rs.responseBody, int_query, m[0]);
        //exit(0);
        rv = to!long(m[0]);
      }else{
        deb && ferr.writefln("Response body: '%s' after --num=\"%s\" not contain any integers.", rs.responseBody, int_query);
        errors && ferr.writefln("\"%s\" - has not digits.", int_query);
        //exit(3);
        rv = 0;
      }
      writefln("%d",rv);
      exit(0);
  }
    
    
  exit(0);
}



void childPostSender( uint deb, uint verbosity, int timeout, string server, string content_type ){
  
  auto chunks = new Generator!( immutable(ubyte)[] )({
    for( bool run=true; run;){
        receive(
            ( immutable(ubyte)[] chunk){ 
                if (deb>2) stderr.writefln("Generator yeld chunk:\n%s", chunk);
                else if (deb>1) stderr.write(">");
                
                yield( chunk); 
            }, // получили часть данных
            ( bool stop){ 
                deb && stderr.writefln("Received %s (mean 'stop')", stop);
                run=false; 
            }, // данные закончились
            ( Variant v){ stderr.writefln("Unexpected value received from owner thread: %s", v); },
        );
    }
  });

  auto rq = Request();
  rq.timeout = timeout.seconds;
  if (verbosity) rq.verbosity = verbosity;
  rq.useStreaming = true;
  deb && stderr.writeln("Start post sending...");
  try{
   auto rs = rq.post( server, chunks, content_type);
   auto stream = rs.receiveAsRange();

   if (deb>2) while(!stream.empty) { // text bebug
      stderr.writefln("Received +%d bytes ( %d / %d )", stream.front.length, rq.contentReceived, rq.contentLength);
      stdout.write( cast(string)stream.front );
      stream.popFront;
   }
   else if (deb>1) while(!stream.empty) {
      stderr.write("."); // like a progressbar
      stdout.write( cast(string)stream.front );
      stream.popFront;
   }
   else while(!stream.empty) {
      stdout.write( cast(string)stream.front );
      stream.popFront;
   }
  
   ownerTid.send( rs.code);
  }
//  catch(TimeoutException e){
//    stderr.writefln("Timeout error. (timeout=%s)", timeout );
//    exit(1);
//  }
  catch(Throwable e){
    stderr.writefln("Error: %s", e.msg);
    exit(1);
  }
}



