'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config()
const port = Number(process.argv[2]);
const hbase = require('hbase')
const path = require('path');

const url = new URL(process.argv[3]);

var hclient = hbase({
	host: url.hostname,
	path: url.pathname ?? "/",
	port: url.port ?? 'http' ? 80 : 443,
	protocol: url.protocol.slice(0, -1),
	encoding: 'latin1',
	auth: process.env.HBASE_AUTH
});

function counterToNumber(c) {
	return Number(Buffer.from(c, 'latin1').readBigInt64BE());
}

function floatToNumber(c) {
	// For handling the avg_duration_days which is a float
	const buffer = Buffer.from(c, 'latin1');
	return buffer.readDoubleLE(0);
}

function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		const column = item['column'].split(':')[1]; // Remove the 'city:' prefix
		if (column === 'avg_duration_days') {
			stats[column] = floatToNumber(item['$']);
		} else {
			stats[column] = counterToNumber(item['$']);
		}
	});
	return stats;
}

function rowToYearlyMap(row) {
	var stats = {}
	row.forEach(function (item) {
		const [family, column] = item['column'].split(':');
		stats[`avg_duration_${column}`] = floatToNumber(item['$']);
	});
	return stats;
}

function encodeFloat(value) {
    const buffer = Buffer.alloc(8);
    buffer.writeDoubleLE(value);
    return buffer;
}

// Serve static files from the public directory
app.use(express.static(path.join(__dirname, 'public')));

// Add security headers middleware
app.use((req, res, next) => {
    res.setHeader(
        'Content-Security-Policy',
        "default-src 'self'; " +
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; " +
        "font-src 'self' https://fonts.gstatic.com; " +
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' blob:; " +
        "img-src 'self' blob: data:;"
    );
    next();
});

app.get('/location_stats.html', function (req, res) {
    const location = req.query['location'];
    if (!location) {
        res.status(400).send('Location parameter is required');
        return;
    }
    
    console.log(`Fetching stats for location: ${location}`);
    
    try {
        hclient.table('nagachakradharg_linkedin_location_heat_map_hbase').row(location).get(function (err, cells) {
            if (err) {
                console.error('Error fetching location data:', err);
                res.status(503).json({
                    error: 'Service temporarily unavailable',
                    details: err.message
                });
                return;
            }
            
            if (!cells || cells.length === 0) {
                res.status(404).json({
                    error: 'Location not found',
                    location: location
                });
                return;
            }

            try {
                const locationStats = {};
                cells.forEach(function (cell) {
                    const column = cell['column'].split(':')[1];
                    // Add additional error checking for cell values
                    if (cell['$'] !== undefined && cell['$'] !== null) {
                        locationStats[column] = cell['$'].toString();
                    } else {
                        locationStats[column] = 'N/A';
                    }
                });

                console.log('Location stats:', locationStats);

                // Add error handling for template reading
                let template;
                try {
                    template = filesystem.readFileSync("location_result.mustache").toString();
                } catch (templateError) {
                    console.error('Error reading template:', templateError);
                    res.status(500).json({
                        error: 'Template error',
                        details: templateError.message
                    });
                    return;
                }

                const html = mustache.render(template, {
                    location: location,
                    job_count: locationStats.job_count || 'N/A',
                    avg_duration_days: locationStats.avg_duration_days ? parseFloat(locationStats.avg_duration_days).toFixed(2) : 'N/A',
                    remote_job_count: locationStats.remote_job_count || 'N/A',
                    unique_companies: locationStats.unique_companies || 'N/A',
                    top_job_type: locationStats.top_job_type || 'N/A'
                });
                
                res.setHeader('Content-Type', 'text/html');
                res.send(html);
            } catch (renderError) {
                console.error('Error rendering template:', renderError);
                res.status(500).json({
                    error: 'Rendering error',
                    details: renderError.message
                });
            }
        });
    } catch (error) {
        console.error('Error in location stats endpoint:', error);
        res.status(500).json({
            error: 'Server error',
            details: error.message
        });
    }
});

app.listen(port);
