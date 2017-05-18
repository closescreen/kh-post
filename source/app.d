import std.stdio, std.string, std.getopt, core.stdc.stdlib, std.regex, std.algorithm, std.concurrency, requests;
import std.process, std.functional, std.conv, core.thread;

void main(string[] args) {
  auto synopsis = "Communicates with ClickHouse server via HTTP POST.\nUsage: echo abc | kh-post options...";
  auto samples = "
# explicity define server option:

kh-post -s\"kh1\" -q'show tables from rnd600' 

# as environment variable:
export kh_server=kh1.myorg

t=\"rnd600.test3\" 

kh-post -q\"CREATE TABLE IF NOT EXISTS $t ( sid UInt64,  name String,  d Date DEFAULT today()) ENGINE = MergeTree(d, sid, 8192)\"
#OR: echo \"CREATE TABLE IF NOT EXISTS $t ( sid UInt64,  name String,  d Date DEFAULT today()) ENGINE = MergeTree(d, sid, 8192)\" | kh-post -i

kh-post -q\"show create table $t\"
#OR: echo \"show create table $t\" | kh-post -i

echo -e \"1\\t'site1'\" | kh-post -i -q\"insert into $t (sid,name)\" -ftsv

kh-post -q\"select * from $t\" --if \"exists table $t\" -ftsvr
";
  auto server = environment.get("kh_server", "");
  string query;
  auto read_stdin = false;
  string if_query;
  string ifnot_query;
  auto yes_query = "";
  auto yes_re_str_default = `[^0\s]`;
  auto yes_re_str = "";
  auto proto = "http://";
  auto port = "8123";
  auto chunk_size = 100_000_000;
  auto content_type = "application/binary";	
  auto deb = 0;
  auto verbosity = 0;
  bool msg = false;
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
	std.getopt.config.caseSensitive,
	"FORCE", "if text in '-q|--if|--ifnot|-y' has /drop|alter/i, then you must enable this flag (stdin will not checked)", &danger_admited,
	std.getopt.config.caseInsensitive,
	"stdin|i", "read first part of query from '-q', and second part from STDIN.", &read_stdin,
	"if", "execute '--query' only if --if=<query> result match m/[^0\\s]/. Otherwise exit(2). '--if' cannot read stdin.", &if_query,
	"ifnot", "like '--if' but negates result. Starts after '--if'. Both '--if' and '--ifnot' allowed at the same time", &ifnot_query,
	"yes|y", "--yes=<sql> executes <sql> (after --if|--ifnot and -q) and exit with 0 if result match by m/[^0\\s]/, and 3 otherwise", &yes_query,
	"regex|r", "match '--yes' result with given regex. exit(0) if matched, exit(3) otherwise", &yes_re_str, 
	"format|f", "phrase: ' FORMAT <format>' will added to --query string", &format,
	"content-type", "content-type header for input data [%s]".format(content_type), &content_type,
	"chunk-size", "chunk size in bytes [%s]".format(chunk_size), &chunk_size,
	"deb|d+", "increase debug messages level. Multiple '-d' allowed.", &deb,
	"verbosity|v+", "increment verbosity level for http requests. Multiple '-v' allowed to increase it.", &verbosity,
	"msg|m", "print to stderr error message if '--if|--ifnot|--yes' fail.", &msg,
	"expect", "expected http codes. List of codes. %s".format(expect), &expect,
	"port","server port [%s]".format(port), &port,
	"proto","server protocol [%s]".format(proto), &proto,
    );
    if( cli.helpWanted ) 
        defaultGetoptPrinter( synopsis, cli.options), 
        writefln("Format in --format maybe:\n%s \nSample:%s", formats, samples), exit(0);
  }
  catch(Throwable o) stderr.writeln(o.msg), exit(1);

  if( empty( server)) stderr.writeln("kh-post: server not defined!"), exit(1);
  else if( !matchFirst( server, regex("https?://"))) server = proto ~ server; // 'kh.myorg' --> 'http://kh.myorg'
  !matchFirst( server, regex(`:\d+$`)) && ( server ~= ":" ~ port ); // 'http://kh.myorg' --> 'http://kh.myorg:8123'
  
  deb && stderr.writefln( "deb:%s, verb:%s, server: %s, expect_codes: %s, chunk_size: %s, content_type: %s", 
    deb, verbosity, server, expect, chunk_size, content_type);

  if (!read_stdin && query.empty && yes_query.empty) stderr.writeln("Either -i or -q or -y must be defined."), exit(1);

  if ( yes_re_str.not!empty && yes_query.empty ) stderr.writeln("--regex=<re> without --yes=<sql> not allowed."), exit(1);
  if ( yes_re_str.empty ) yes_re_str = yes_re_str_default;
  
  auto timer = 0;
  
  foreach (text; [query, yes_query, if_query, ifnot_query ]){
    if ( matchFirst( text, regex(`drop|alter`, "i")) && !danger_admited ){ 
      timer = 15;
      stderr.writefln("Ctrl+C to abort (wait %s sec). Use --FORCE to disable timer.\n%s\n", timer, text);
      break;
    }   
  }
  
  if (timer)
    foreach (i; 1..timer+1 ){ 
      Thread.sleep( dur!("seconds")( 1 ) );
      stderr.writef("%s ", i);
    }
  
  
  auto fin = stdin;
  auto fout = stdout;
  auto ferr = stderr;
  
  if ( if_query.not!empty || ifnot_query.not!empty ){
    auto rq = Request();
    if (verbosity) rq.verbosity=verbosity;
    if ( if_query.not!empty ){
      auto rs = rq.post( server, if_query, content_type);
      if( !expect.canFind( rs.code)){
        ferr.writefln("Response code: %s\n--ifquery: %s.\n%s Stop executing.", rs.code, if_query, rs.responseBody );
        exit(1);
      }
      if ( ! rs.responseBody.to!string.matchFirst( r"[^0\s]") ){
        deb && ferr.writefln(
            "Response body: %s\n after --if=\"%s\" does not contain any 'yes' symbols. Stop executing.", rs.responseBody, if_query);
        msg && ferr.writefln(
            "\"%s\" - returns NO (--if failed).", if_query);

        exit(2);
      }
    }

    if ( ifnot_query.not!empty ){
      auto rs = rq.post( server, ifnot_query, content_type);
      if( !expect.canFind( rs.code)){
        ferr.writefln("Response code: %s\n--ifnot query: %s.\n%s Stop executing.", rs.code, ifnot_query, rs.responseBody );
        exit(1);
      }
      if ( rs.responseBody.to!string.matchFirst( r"[^0\s]") ){
        deb && ferr.writefln(
            "Response body:\n%s\n after --ifnot=\"%s\" contain smth 'yes' symbols. Stop executing.", rs.responseBody, ifnot_query);
        msg && ferr.writefln(
            "\"%s\" - returns YES (--ifnot failed).", ifnot_query);
            
        exit(2);
      }
    }
  }
  
  if ( query.not!empty || read_stdin ){
    deb>1 && ferr.writeln("spawn child process");
    auto childTid = spawn( &childPostSender, deb, verbosity, server, content_type );

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
      if (verbosity) rq.verbosity = verbosity;
      auto rs = rq.post( server, yes_query, content_type);
      if( !expect.canFind( rs.code)){
        ferr.writefln("Response code: %s\n--yes query: %s.\n%s Stop executing.", rs.code, yes_query, rs.responseBody );
        exit(1);
      }
      if ( rs.responseBody.to!string.matchFirst( yes_re_str )){ //r"[^0\s]") ){
        deb && ferr.writefln("Response body: \n%s\n after --yes=\"%s\" contain smth 'yes' symbols.", rs.responseBody, yes_query);
        exit(0);
      }else{
        deb && ferr.writefln("Response body: \n%s\n after --yes=\"%s\" not contain any 'yes' symbols.", rs.responseBody, yes_query);
        msg && ferr.writefln("\"%s\" - returns NO.", yes_query);
        exit(3);
      }
    }  
    
  exit(0);
}



void childPostSender( uint deb, uint verbosity, string server, string content_type ){
  
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
  if (verbosity) rq.verbosity = verbosity;
  rq.useStreaming = true;
  deb && stderr.writeln("Start post sending...");
  auto rs = rq.post( server, chunks, content_type);
  auto stream = rs.receiveAsRange();

  if (deb>2) while(!stream.empty) { // text bebug
      deb>2 && stderr.writefln("Received +%d bytes ( %d / %d )", stream.front.length, rq.contentReceived, rq.contentLength);
      stdout.write( cast(string)stream.front );
      stream.popFront;
  }
  else if (deb>1) while(!stream.empty) {
      deb>2 && stderr.write("."); // like a progressbar
      stdout.write( cast(string)stream.front );
      stream.popFront;
  }
  else while(!stream.empty) {
      stdout.write( cast(string)stream.front );
      stream.popFront;
  }
  
  ownerTid.send( rs.code);
}



