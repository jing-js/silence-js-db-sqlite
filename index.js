'use strict';

const silence = require('silence-js');
const BaseSQLDatabaseStore = silence.BaseSQLDatabaseStore;
const sqlite = require('sqlite3');
const path = require('path');
const CWD = process.cwd();
const fs = require('fs');

function createDir(dir) {
  return new Promise((resolve, reject) => {
    fs.access(dir, err => { // 首先检测是否已经存在
      if (err) { 
        // 如果不存在，先创建其父亲文件夹。这是一个递归的过程
        let parentDir = path.dirname(dir);
        createDir(parentDir).then(() => {
          console.log(dir);
          fs.mkdir(dir, err => {
            if (err) {
              reject(err);
            } else {
              resolve();
            }
          });
        }, reject);
      } else { // 如果已经存在则直接返回
        resolve();
      }
    });
  })
}

class SqliteDatabaseStore extends BaseSQLDatabaseStore {
  constructor(config, logger) {
    super(logger, 'sqlite');
    this.db = null;
    this.file = config.file ? path.resolve(CWD, config.file) : ':memory:';
  }
  init() {
    return createDir(path.dirname(this.file)).then(() => {
      return new Promise((resolve, reject) => {
        this.db = new sqlite.Database(this.file, (err) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });
    });
  }
  close() {
    return new Promise((resolve, reject) => {
      this.db.close(err => {
        if(err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }
  initField(field) {
    super.initField(field);
    if (/^INT/.test(field.type)) {
      field.type = 'INTEGER';
    }
  }
  genCreateTableSQL(Model) {
    let segments = [];
    let pk = null;
    let indexFields = [];
    let indices = Model.indices;
    let fields = Model.fields;
    let name = Model.table;

    if (typeof indices === 'object' && indices !== null) {
      for(let k in indices) {
        indexFields.push({
          name: k,
          value: Array.isArray(indices[k]) ? indices[k].join(',') : indices[k]
        });
      }
    }
    for(let i = 0; i < fields.length; i++) {

      let field = fields[i];
      let sqlSeg = `\`${field.name}\` ${field.type.toUpperCase()}`;

      if (field.require || field.primaryKey) {
        sqlSeg += ' NOT NULL';
      }

      if (field.hasOwnProperty('defaultValue')) {
        sqlSeg += ` DEFAULT '${field.defaultValue}'`;
      }

      if (field.primaryKey) {
        sqlSeg += ' PRIMARY KEY';
      }

      if (field.autoIncrement === true) {
        sqlSeg += ' AUTOINCREMENT';
      }


      if (field.unique === true) {
         sqlSeg += ' UNIQUE';
      }
      if (field.index === true) {
        indexFields.push({
          name: field.name,
          value: field.name
        });
      }

      segments.push(sqlSeg);
    }

    //todo support foreign keys

    let sql = `CREATE TABLE \`${name}\` (\n  ${segments.join(',\n  ')});`;
    if (indexFields.length > 0) {
      indexFields.forEach(index => {
        sql += `CREATE INDEX \`${index.name}_INDEX\` on ${name}(${index.value});`;
      });
    }

    return sql;

  }
  exec(queryString, queryParams) {
    this.logger.debug(queryString);
    this.logger.debug(queryParams);
    return new Promise((resolve, reject) => {
      this.db.run(queryString, queryParams, function(err) {
        if (err) {
          reject(err);
        } else {
          resolve({
            affectedRows: this.changes,
            insertId: this.lastID
          });
        }
      });
    });
  }
  query(queryString, queryParams) {
    this.logger.debug(queryString);
    this.logger.debug(queryParams);
    return new Promise((resolve, reject) => {
      this.db.all(queryString, queryParams, function(err, rows) {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      });
    });
  }
}

module.exports = SqliteDatabaseStore;
