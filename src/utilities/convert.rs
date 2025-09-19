// Convert the u32 to 4 u8, in big endian style
pub fn u32_to_u8set(x:u32) -> [u8;4] {
    let b1 : u8 = ((x >> 24) & 0xff) as u8;
    let b2 : u8 = ((x >> 16) & 0xff) as u8;
    let b3 : u8 = ((x >> 8) & 0xff) as u8;
    let b4 : u8 = (x & 0xff) as u8;
    return [b1, b2, b3, b4]
}

pub fn u8set_to_u32(x:[u8;4]) -> u32 {
    let b = u32::from_be_bytes(x);
    return b;
}