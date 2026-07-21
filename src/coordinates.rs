const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;
const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;

const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;
const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;

const EARTH_RADIUS: f64 = 6372797.560856;

pub(crate) fn are_valid_coords(longitude: f64, latitude: f64) -> bool {
    longitude >= MIN_LONGITUDE
        && longitude <= MAX_LONGITUDE
        && latitude >= MIN_LATITUDE
        && latitude <= MAX_LATITUDE
}

fn spread_u32_to_u64(n: u32) -> u64 {
    let mut v = n as u64;

    // Bitwise operations to spread 32 bits into 64 bits with zeros in-between
    v = (v | (v << 16)) & 0x0000FFFF0000FFFF;
    v = (v | (v << 8)) & 0x00FF00FF00FF00FF;
    v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F;
    v = (v | (v << 2)) & 0x3333333333333333;
    v = (v | (v << 1)) & 0x5555555555555555;

    v
}

fn compact_u64_to_u32(n: u64) -> u32 {
    // Keep only the bits in even positions
    // Before masking: w1   v1  ...   w2   v16  ... w31  v31  w32  v32
    // After masking: 0   v1  ...   0   v16  ... 0  v31  0  v32
    let mut v = n & 0x5555555555555555;

    // Reverse the spreading process by shifting and masking
    // Before compacting: 0   v1  ...   0   v16  ... 0  v31  0  v32
    // After compacting: v1  v2  ...  v31  v32
    v = (v | (v >> 1)) & 0x3333333333333333;
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F;
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF;
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF;
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF;

    return v as u32;
}

/**
 * Source: https://github.com/codecrafters-io/redis-geocoding-algorithm
 */
pub(crate) fn encode_coords(longitude: f64, latitude: f64) -> u64 {
    let normalized_longitude =
        ((1 << 26) as f64 * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE) as u32;
    let normalized_latitude =
        ((1 << 26) as f64 * (latitude - MIN_LATITUDE) / LATITUDE_RANGE) as u32;

    // Before spread: x1  x2  ...  x31  x32
    // After spread:  0   x1  ...   0   x16  ... 0  x31  0  x32
    let x = spread_u32_to_u64(normalized_latitude);
    let y = spread_u32_to_u64(normalized_longitude);

    // The y value is then shifted 1 bit to the left.
    // Before shift: 0   y1   0   y2 ... 0   y31   0   y32
    // After shift:  y1   0   y2 ... 0   y31   0   y32   0
    let y_shifted = y << 1;

    // Before bitwise OR (x): 0   x1   0   x2   ...  0   x31    0   x32
    // Before bitwise OR (y): y1  0    y2  0    ...  y31  0    y32   0
    // After bitwise OR     : y1  x1   y2  x2   ...  y31  x31  y32  x32
    x | y_shifted
}

/**
 * Source: https://github.com/codecrafters-io/redis-geocoding-algorithm
 */
pub(crate) fn decode_coords(geo_code: u64) -> (f64, f64) {
    let y = geo_code >> 1;
    let x = geo_code;

    let grid_longitude_number = compact_u64_to_u32(y);
    let grid_latitude_number = compact_u64_to_u32(x);

    // Calculate the grid boundaries
    let grid_longitude_min =
        MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number as f64 / (1 << 26) as f64) as f64;
    let grid_longitude_max = MIN_LONGITUDE
        + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64 / (1 << 26) as f64) as f64;
    let grid_latitude_min =
        MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number as f64 / (1 << 26) as f64) as f64;
    let grid_latitude_max = MIN_LATITUDE
        + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64 / (1 << 26) as f64) as f64;

    // Calculate the center point of the grid cell
    let latitude = (grid_latitude_min + grid_latitude_max) / 2.0;
    let longitude = (grid_longitude_min + grid_longitude_max) / 2.0;

    return (longitude, latitude);
}

/* Calculate distance using simplified haversine great circle distance formula.
 * Given longitude diff is 0 the asin(sqrt(a)) on the haversine is asin(sin(abs(u))).
 * arcsin(sin(x)) equal to x when x ∈[−𝜋/2,𝜋/2]. Given latitude is between [−𝜋/2,𝜋/2]
 * we can simplify arcsin(sin(x)) to x.
 */
fn geohash_get_lat_distance(lat1d: f64, lat2d: f64) -> f64 {
    return EARTH_RADIUS * (lat2d.to_radians() - lat1d.to_radians()).abs();
}

/**
 * Source: https://github.com/redis/redis/blob/4322cebc1764d433b3fce3b3a108252648bf59e7/src/geohash_helper.c#L228C1-L228C72
 */
pub(crate) fn geohash_get_distance(lon1d: f64, lat1d: f64, lon2d: f64, lat2d: f64) -> f64 {
    let lon1r = lon1d.to_radians();
    let lon2r = lon2d.to_radians();
    let lat1r = lat1d.to_radians();
    let lat2r = lat2d.to_radians();

    let v = ((lon2r - lon1r) / 2.0).sin();
    /* if v == 0 we can avoid doing expensive math when lons are practically the same */
    if v == 0.0 {
        return geohash_get_lat_distance(lat1d, lat2d);
    }

    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;

    2.0 * EARTH_RADIUS * a.sqrt().asin()
}
