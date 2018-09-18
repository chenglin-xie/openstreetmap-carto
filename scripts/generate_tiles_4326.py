#!/usr/bin/env python
from math import pi, cos, sin, log, exp, atan
from subprocess import call
import sys, os
from Queue import Queue

import threading

import mapnik

DEG_TO_RAD = pi/180
RAD_TO_DEG = 180/pi
TILE_SIZE  = 256
TILE_SIZEF = 256.0

# Default number of rendering threads to spawn, should be roughly equal to number of CPU cores available
NUM_THREADS = 6

longlat = mapnik.Projection('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')


class GeographicProjection:
    def __init__(self, levels=21):
        self.YPX = []
        self.XPX = []
        for z in range(0, levels):
            ncols = 2**(z+1)
            nrows = 2**z
            self.YPX.append(nrows*TILE_SIZE);
            self.XPX.append(ncols*TILE_SIZE);
                
    def fromLLtoPixel(self, ll, zoom):
        lon = ll[0]
        lat = ll[1]
        xpixels = self.XPX[zoom]
        ypixels = self.YPX[zoom]

        px = round((lon + 180.0) / 360.0 * xpixels)
        py = round((lat + 90.0) / 180.0 * ypixels)
        return (px,py)
     
    def fromPixelToLL(self, px, zoom):
        x = px[0]
        y = px[1]
        xpixels = float(self.XPX[zoom])
        ypixels = float(self.YPX[zoom])

        lon = (x / xpixels) * 360.0 - 180.0
        lat = (y / ypixels) * 180.0 - 90.0
        return (lon,lat)


class RenderThread:
    def __init__(self, tile_dir, mapfile, q, printLock, maxZoom):
        self.tile_dir = tile_dir
        self.q = q
        self.m = mapnik.Map(TILE_SIZE, TILE_SIZE)
        self.printLock = printLock
        # Load style XML
        mapnik.load_map(self.m, mapfile, True)

        # Obtain <Map> projection
        self.prj = longlat;
        self.m.srs = longlat.params();
        # Projects between tile pixel co-ordinates and LatLong (EPSG:4326)
        self.tileproj = GeographicProjection(maxZoom+1)

    def render_tile(self, tile_uri, x, y, z):
        # Calculate pixel positions of bottom-left & top-right
        p0 = (x * TILE_SIZE, y * TILE_SIZE)
        p1 = ((x + 1) * TILE_SIZE, (y + 1) * TILE_SIZE)       

        # Convert to LatLong (EPSG:4326)
        l0 = self.tileproj.fromPixelToLL(p0, z);
        l1 = self.tileproj.fromPixelToLL(p1, z);

        # These are already in EPSG:4326, no need to project
        c0 = mapnik.Coord(l0[0], l0[1])
        c1 = mapnik.Coord(l1[0], l1[1])

        # Bounding box for the tile
        bbox = mapnik.Box2d(c0.x, c0.y, c1.x, c1.y)
        #print bbox
        render_size = TILE_SIZE
        self.m.resize(render_size, render_size)
        self.m.zoom_to_box(bbox)
        self.m.buffer_size = 128

        # Render image with default Agg renderer
        im = mapnik.Image(render_size, render_size)
        mapnik.render(self.m, im)
        im.save(tile_uri, 'png256')

    def loop(self):
        while True:
            #Fetch a tile from the queue and render it
            r = self.q.get()
            if (r == None):
                self.q.task_done()
                break
            else:
                (name, tile_uri, x, y, z) = r

            exists= ""
            if os.path.isfile(tile_uri):
                exists= "exists"
            else:
                self.render_tile(tile_uri, x, y, z)
            bytes=os.stat(tile_uri)[6]
            empty= ''
            if bytes == 103:
                empty = " Empty Tile "
            self.printLock.acquire()
            print name, ":", z, x, y, exists, empty
            self.printLock.release()
            self.q.task_done()


def render_tiles(bbox, mapfile, tile_dir, minZoom=0, maxZoom=20, name="unknown", num_threads=NUM_THREADS):
    print "render_tiles(", bbox, mapfile, tile_dir, minZoom, maxZoom, name, ")"

    # Launch rendering threads
    queue = Queue(32)
    printLock = threading.Lock()
    renderers = {}
    for i in range(num_threads):
        renderer = RenderThread(tile_dir, mapfile, queue, printLock, maxZoom)
        render_thread = threading.Thread(target=renderer.loop)
        render_thread.start()
        print "Started render thread %s" % render_thread.getName()
        renderers[i] = render_thread

    if not os.path.isdir(tile_dir):
         os.mkdir(tile_dir)

    gprj = GeographicProjection(maxZoom+1) 
    ll0 = (bbox[0],bbox[3])
    ll1 = (bbox[2],bbox[1])

    for z in range(minZoom, maxZoom + 1):
        px0 = gprj.fromLLtoPixel(ll0, z)
        px1 = gprj.fromLLtoPixel(ll1, z)

        # check if we have directories in place
        zoom = "%s" % z
        if not os.path.isdir(tile_dir + zoom):
            os.mkdir(tile_dir + zoom)
            
        for x in range(int(px0[0]/TILE_SIZEF), int(px1[0]/TILE_SIZEF)+1):
            # Validate x co-ordinate
            if (x < 0) or (x > 2**z):
                continue

            # check if we have directories in place
            str_x = "%s" % x
            if not os.path.isdir(tile_dir + zoom + '/' + str_x):
                os.mkdir(tile_dir + zoom + '/' + str_x)
                
            for y in range(int(px1[1]/TILE_SIZEF), int(px0[1]/TILE_SIZEF)+1):
                # Validate x co-ordinate
                if (y < 0) or (y >= 2**z):
                    continue

                #str_y = "%s" % y
                str_y = "%s" % ((2**z-1) - y)

                tile_uri = tile_dir + zoom + '/' + str_x + '/' + str_y + '.png'

                # Submit tile to be rendered into the queue
                t = (name, tile_uri, x, y, z)
                try:
                    queue.put(t)
                except KeyboardInterrupt:
                    raise SystemExit("Ctrl-c detected, exiting...")

    # Signal render threads to exit by sending empty request to queue
    for i in range(num_threads):
        queue.put(None)
    # wait for pending rendering jobs to complete
    queue.join()
    for i in range(num_threads):
        renderers[i].join()


if __name__ == "__main__":
    home = os.environ['HOME']
    try:
        mapfile = os.environ['MAPNIK_MAP_FILE']
    except KeyError:
        mapfile = home + "/src/openstreetmap-carto/mapnik4326.xml"
    try:
        tile_dir = os.environ['MAPNIK_TILE_DIR']
    except KeyError:
        tile_dir = home + "/data/osm/tiles/"

    if not tile_dir.endswith('/'):
        tile_dir = tile_dir + '/'

    # Start with an overview
    # World
    bbox = (-180.0, -90.0, 180.0, 90.0)
    render_tiles(bbox, mapfile, tile_dir, 0, 5, "World")

    # North America
    #bbox = (-169.0, 23.0, -48.0, 85.0)
    #render_tiles(bbox, mapfile, tile_dir, 6, 10, "NorthAmerica")

    # California
    #bbox = (-124.5, 31.578535, -113.95752, 43.934977)
    #render_tiles(bbox, mapfile, tile_dir, 11, 20, "California")
