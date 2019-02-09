/// Generate random string effeciently
/// Ref: https://colobu.com/2018/09/02/generate-random-string-in-Go/
const LETTER_IDX_BITS: u64 = 6;
const LETTER_IDX_MASK: u64 = (1 << LETTER_IDX_BITS) - 1;
const LETTER_IDX_MAX: u64 = 64 / LETTER_IDX_BITS;

pub fn gen_rand_string(n: usize) -> String {
    let letters: Vec<char> = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        .to_owned()
        .chars()
        .collect();
    let mut b = vec!['x'; n];
    let mut i = n - 1;
    let mut cache = rand::random::<u64>();
    let mut remain = LETTER_IDX_MAX;
    while i as i64 >= 0 {
        if remain == 0 {
            cache = rand::random::<u64>();
            remain = LETTER_IDX_MAX;
        }
        let idx: u32 = (cache & LETTER_IDX_MASK) as u32;
        if idx < letters.len() as u32 {
            b[i] = letters[idx as usize];
            i -= 1;
        }
        cache >>= LETTER_IDX_BITS;
        remain -= 1;
    }

    return b.iter().cloned().collect::<String>();
}
