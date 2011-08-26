
var crypto = require('crypto');
var path = require('path');
var events = require('events');
var fs = require('fs');
var _ = require('underscore');
var mkdirsSync = require('mkdir').mkdirsSync;
var gzip = require('gzip');

// *************************************************************************************************

/**
 * Disk cache manages an cache on disk and optionally in memory.
 *
 * Any object which can be converted to JSON can be stored.
 */
function DiskCache(cachePath, useDisk, useMem, useGzip) {
    events.EventEmitter.call(this);

	this.cachePath = cachePath;
	this.useMem = useMem;
	this.useDisk = useDisk;
	this.useGzip = useGzip;
	this.memCache = {};
	this.locks = {};
	if (cachePath) {
		mkdirsSync(cachePath);
	}
}
exports.DiskCache = DiskCache;

function subclass(cls, supercls, proto) {
    cls.super_ = supercls;
    cls.prototype = Object.create(supercls.prototype, {
        constructor: {value: cls, enumerable: false}
    });
    _.extend(cls.prototype, proto);
}

subclass(DiskCache, events.EventEmitter, {
	/**
	 * Caches data for a url with an optional category.
	 */
	store: function(url, data, category, cb) {
		if (typeof(category) == "function") { cb = category; category = null; }

		var jsonData = JSON.stringify(data);

		if (this.useMem) {
			if (this.useGzip && data.body) {
	        	gzip(data.body, _.bind(function(err, gzipped) {
	        		data.bodyZipped = gzipped;
					this.memCache[url] = data;
					phase2.apply(this);
	        	}, this));
	        } else {
				this.memCache[url] = data;
				phase2.apply(this);
	        }
		} else {
			phase2.apply(this);
		}

		function phase2() {
			if (this.useDisk) {
				var filePath = this.pathForURL(url, category);
				// console.log('writing',url);
				fs.writeFile(filePath, jsonData, 'utf8', _.bind(function(err) {
					// console.log('wrote',url);
					phase3.apply(this, [err]);
				}, this));
			} else {
				phase3.apply(this);
			}
		}

		function phase3(err) {
			if (cb) {
				cb(err, data);
			}
			this.unlock(url, data);
		}
	},

	/**
	 * Loads cached data for a url with an optional category.
	 */
	load: function(url, category, cb) {
		if (typeof(category) == "function") {cb = category; category = null; }

		var locks = this.locks[url];
		if (locks) {
			// console.log('wait for lock on', url);
			locks.push(cb);
		} else {
			if (url in this.memCache) {
				cb(0, this.memCache[url]);
			} else if (this.useDisk) {
				var filePath = this.pathForURL(url, category);
				// console.log('try to load', filePath, 'for', url);
				fs.readFile(filePath, _.bind(function(err, jsonData) {
					if (err) return cb ? cb(err) : 0;

					var data = JSON.parse(jsonData);

					if (this.useMem) {
						if (this.useGzip && data.body) {
				        	gzip(data.body, _.bind(function(err, gzipped) {
				        		data.bodyZipped = gzipped;
								this.memCache[url] = data;
								cb(0, data, true);
				        	}, this));
				        } else {
							this.memCache[url] = data;
							cb(0, data, true);
				        }
					} else {
						cb(0, data, true);
					}
				}, this));				
			} else {
				cb(new Error("Not found in cache"));
			}
		}
	},
	
	/**
	 * Locks a url so that it can't be accessed until it is stored.
	 */	
	lock: function(url) {
		// console.log('lock', url);
		this.locks[url] = [];
	},

	/**
	 * Unlocks a url and dispatches its new value to callbacks waiting on the lock.
	 */	
	unlock: function(url, data) {
		// console.log('unlock', url);
		var callbacks = this.locks[url];
		if (callbacks) {
			delete this.locks[url];

			callbacks.forEach(function(cb) {
				cb(0, data);
			});
		}
	},

	/**
	 * Invalidates a given url when changes affect dependent files.
	 */
	monitor: function(url, category, dependencies, cb) {
		_.each(dependencies, _.bind(function(depPath) {
			fs.lstat(depPath, _.bind(function(err, stat) {
				if (err) { if (cb) cb(err); return; }

				var mtime = stat.mtime.getTime();
				fs.watchFile(depPath, _.bind(function() {
					fs.lstat(depPath, _.bind(function(err, stat) {
						if (stat.mtime.getTime() != mtime) {
							D&&D("Modified", depPath, stat.mtime);
							this.unmonitor(url, dependencies);
							this.remove(url, category);
							if (cb) { cb(0, url); }
						}
					}, this));
				}, this));					
			}, this));
		}, this));
	},

	/**
	 * Stops monitoring changes to files for a given url.
	 */
	unmonitor: function(url, dependencies) {
		_.each(dependencies, function(depPath) {
			fs.unwatchFile(depPath);
		});
		this.emit('unmonitor', {url: url});
	},

	/**
	 * Removes cached data for a url.
	 */
	remove: function(url, category, cb) {
		if (typeof(category) == "function") {cb = category; category = null; }

		var filePath = this.pathForURL(url, category);
		// console.log('remove', filePath, 'for', url);
		fs.unlink(filePath, cb);
		
		if (this.useMem) {
			delete this.memCache[url];
		}
	},

	/**
	 * Removes all files in the cache or within a category.
	 */
	removeAll: function(category) {
		var cachePath = category ? path.join(this.cachePath, category) : this.cachePath;
		try {
			var fileNames = fs.readdirSync(cachePath);
			_.each(fileNames, _.bind(function(fileName) {
				var filePath = path.join(cachePath, fileName);
				fs.unlink(filePath);
			}, this));
		} catch (exc) {
			// console.error(exc);
		}

		if (this.useMem) {
			this.memCache = {};
		}
	},

	/**
	 * Gets the key used for file names in the disk cache.
	 */
	keyForURL: function(url) {
		var hash = crypto.createHash('md5');
		hash.update(url);
		return hash.digest('hex');
	},

	/**
	 * Gets the path of the file where a url is stored in the disk cache.
	 */
	pathForURL: function(url, category) {
		var cachePath = category ? path.join(this.cachePath, category) : this.cachePath;
		if (category) {
			mkdirsSync(cachePath);
		}
		var key = this.keyForURL(url);
		var fileName = key + '.txt';
		return path.join(cachePath, fileName);
	}
});

exports.DiskCache = DiskCache;
