import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
)

export default async function handler(req, res) {
  try {
    const { data: jobs, error } = await supabase
      .from('ApplyMore')
      .select('id, created_at')
      .eq('is_hidden', false)
      .order('created_at', { ascending: false })

    if (error) throw error

    const baseUrl = 'https://applymore.vercel.app'
    const today = new Date().toISOString().split('T')[0]

    let xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`

    xml += `
  <url>
    <loc>${baseUrl}/</loc>
    <lastmod>${today}</lastmod>
    <changefreq>daily</changefreq>
    <priority>1.0</priority>
  </url>`

    for (const job of jobs) {
      const lastmod = job.created_at ? job.created_at.split('T')[0] : today
      xml += `
  <url>
    <loc>${baseUrl}/job.html?id=${job.id}</loc>
    <lastmod>${lastmod}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>`
    }

    xml += `
</urlset>`

    res.setHeader('Content-Type', 'application/xml')
    res.status(200).send(xml)
  } catch (err) {
    console.error(err)
    res.status(500).send('Internal Server Error')
  }
}
