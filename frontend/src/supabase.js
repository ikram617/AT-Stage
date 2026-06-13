import { createClient } from '@supabase/supabase-js'

const supabaseUrl = 'https://kzdxrojsvfgyuojxzgnu.supabase.co'
const supabaseAnonKey = 'sb_publishable_eQi-v7FpGVfBSWESddINnA_t0Hg4lYO'

export const supabase = createClient(supabaseUrl, supabaseAnonKey)
