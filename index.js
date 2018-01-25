#!/usr/bin/env node

var fs = require('fs');
var minimist = require('minimist');
var zlib = require('zlib');
var pumpify = require('pumpify');
var through2 = require('through2');
var eventToPromise = require('event-to-promise')
var parseCsv = require('csv-parser')
var stripBomStream = require('strip-bom-stream')
var pkg = require('./package.json')


split = function(baseName, max) {
    let out = null;
    let lines = 0;
    let counter = 1;
    
    let proxy = through2.obj(function(chunk, _, done){
        lines = lines + 1;
        if(lines > max) {
            if( out != null ) {
                console.log('Closing stream '+counter);
                out.end();
            }
            lines=1;
            counter++;
        }
        if(lines == 1) {
            out = zlib.createGzip()
            out.pipe(fs.createWriteStream(baseName+'-'+counter));
        }
        if (!out.write(JSON.stringify(chunk)+'\n')) {
            out.once('drain', done);
        } else {
            process.nextTick(done);
        }
    }, function(done){
        console.log('Finally closing');
        out.end(done);
    });

    return proxy;
};

function main(args) {
    var usage = `
        Transform a (optionally compressed) CSV file into multiple (optionally compressed) 
        JSON files splitting them by a defined number of lines (default 10000).

        Usage: `+pkg.name + ` [OPTIONS] [<input file>] 

          -l line_count
            Create smaller files n lines in length.  Default is 10000 lines.

          -p prefix 
            Use <prefix> for output files

          -c, --csv
            Input is a Comma separated file, assumed by default.
        
          -t, --tsv 
            Input is a Tab separated file

          -b
            Don't Strip UTF-8 byte order mark (BOM) from a stream

          -zi
            The input file (or stdin) 
          
          -zo
            Compress the output 
          
          - h, --help 
            Print this.
    `;
    args = minimist(args.slice(2), {
        boolean: ['help', 'tsv', 'csv', 'zi', 'zo'],
        string : ['prefix'],
        alias: {
            help: 'h',
            c: 'csv',
            t: 'tsv',
            p: 'prefix'
          }
    });
    
    if (args.help) {
        console.log(usage);
        return;
    }

    if (typeof args.l != 'number' ) {
        args.l = 10000;
    }

    if (!args.prefix) {
        args.p = args.prefix = 'part';
    } 
    
    let tr = [];
    
    console.log(typeof args._);
    if (args._.length > 0) {
        tr.push(fs.createReadStream(args._[0]));
    } else {
        console.log('Reading from stdin');
        tr.push(process.stdin);
    }

    if (args.zi || true) {
        tr.push(zlib.createGunzip());
    }

    if (!args.b || true) {
        tr.push(stripBomStream());
    }

    tr.push( parseCsv({separator: args.tsv ? '\t' : ','}));
    tr.push(split(args.p, args.l));

    console.log(JSON.stringify(args));

    return eventToPromise(pumpify(tr));
}


main(process.argv);
