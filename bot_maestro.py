import os
import asyncio
import json
import argparse
import datetime
from urllib.parse import urljoin
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from supabase import create_client, ClientOptions
from google import genai
from google.genai import types

semana_actual = datetime.date.today().isocalendar()[1]

load_dotenv(override=True)
opciones = ClientOptions(schema="agente_precios")
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_ANON_KEY"), options=opciones)
client = genai.Client(api_key=os.environ.get("GOOGLE_API_KEY"))

async def extraer_datos_con_ia(lista_textos, marcas_objetivo, nombre_categoria):
    prompt = f"""
    Analizá estos fragmentos de la categoría: '{nombre_categoria}'.
    Buscá estrictamente: {marcas_objetivo}.
    Extraé: marca, modelo y precio_lista (entero).
    cuotas: Si menciona cuotas, extraé solo el número (ej: 6). Si no, respondé 0.
    Respondé ESTRICTAMENTE un array de objetos JSON. Si no hay nada, respondé[].
    TEXTO: {"\n--- PRODUCTO ---\n".join(lista_textos)}
    """
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )
        return json.loads(response.text)
    except: return[]

async def ejecutar_bot_maestro(lista_retailers_filtro=None):
    res_marcas = supabase.table("marcas").select("nombre").execute()
    marcas_string = ", ".join([m['nombre'] for m in res_marcas.data])
    lista_marcas_rapida =[m['nombre'].upper() for m in res_marcas.data]
    
    res_urls = supabase.table("urls_extraccion").select("*, retailers(*), categorias(*)").eq("activo", True).execute()
    tareas =[t for t in res_urls.data if not lista_retailers_filtro or t['retailers']['nombre'].strip().lower() in[r.lower() for r in lista_retailers_filtro]]
    
    async with async_playwright() as p:
        # ¡ATENCIÓN! headless=True es OBLIGATORIO para correr en GitHub
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36",
            permissions=["geolocation"]
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = await context.new_page()
        await context.set_geolocation({"latitude": -34.6037, "longitude": -58.3816})

        for tarea in tareas:
            retailer, categoria, url_actual = tarea['retailers'], tarea['categorias'], tarea['url_base']
            pagina_nro = 1
            
            print(f"\n=====================================================")
            print(f"🏪 RETAILER: {retailer['nombre']} | 📁 CATEGORÍA: {categoria['nombre']}")
            
            try:
                await page.goto(url_actual, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(4)
                await page.keyboard.press("Escape")
                
                while True:
                    print(f"🌐 [Pág {pagina_nro}] Buscando productos...")
                    await page.wait_for_selector(retailer['selector_caja'], state="attached", timeout=20000)
                    
                    for _ in range(10): 
                        await page.evaluate("window.scrollBy(0, 1000)")
                        await asyncio.sleep(0.6)
                    await asyncio.sleep(2)

                    elementos = await page.query_selector_all(retailer['selector_caja'])
                    if not elementos:
                        print("⚠️ No se encontraron cajas de productos.")
                        break

                    datos_brutos =[]
                    for el in elementos:
                        txt = await el.inner_text()
                        if txt and any(m in txt.upper() for m in lista_marcas_rapida):
                            datos_brutos.append(" | ".join(txt.split('\n')))

                    print(f"📦 Se encontraron {len(datos_brutos)} productos de interés.")

                    if datos_brutos:
                        productos = await extraer_datos_con_ia(datos_brutos, marcas_string, categoria['nombre'])
                        if productos:
                            print(f"🔥 Guardando {len(productos)} precios en BD...")
                            for prod in productos:
                                try:
                                    supabase.table("historico_precios").insert({
                                        "retailer_id": retailer['id'], "categoria_id": categoria['id'],
                                        "marca_detectada": prod['marca'].title(), 
                                        "nombre_modelo_completo": prod['modelo'],
                                        "precio_lista": int(prod['precio_lista']),
                                        "cuotas": int(prod.get('cuotas', 0)),
                                        "semana_anio": semana_actual
                                    }).execute()
                                except: pass

                    # LÍMITE MANUAL (Circuit Breaker)
                    limite_paginas = tarea.get('max_paginas')
                    if limite_paginas and pagina_nro >= limite_paginas:
                        print(f"🛑 Límite manual alcanzado ({limite_paginas} págs). Fin.")
                        break

                    # PAGINACIÓN
                    btn_sig = await page.query_selector(retailer['selector_siguiente'])
                    if btn_sig:
                        deshabilitado = await btn_sig.evaluate("node => node.disabled || node.getAttribute('aria-disabled') === 'true' || node.className.toLowerCase().includes('disabled')")
                        if deshabilitado: break

                        if retailer['tipo_paginacion'] == "ENLACE_SIGUIENTE":
                            link = await btn_sig.get_attribute("href") or await (await btn_sig.query_selector("a")).get_attribute("href") if await btn_sig.query_selector("a") else None
                            if link and link not in ["javascript:void(0);", "#"]:
                                url_actual = urljoin(page.url, link)
                                pagina_nro += 1
                                await page.goto(url_actual, wait_until="domcontentloaded", timeout=60000)
                                await asyncio.sleep(5)
                                continue
                        elif retailer['tipo_paginacion'] == "PARAMETRO_URL":
                            pagina_nro += 1
                            parametro = "p=" if "rodo" in retailer['nombre'].lower() else "page="
                            separador = "&" if "?" in tarea['url_base'] else "?"
                            url_actual = f"{tarea['url_base']}{separador}{parametro}{pagina_nro}"
                            await page.goto(url_actual, wait_until="domcontentloaded", timeout=60000)
                            await asyncio.sleep(5)
                            continue
                        elif retailer['tipo_paginacion'] == "CLICK_AJAX":
                            await btn_sig.evaluate("node => node.click()")
                            pagina_nro += 1
                            await asyncio.sleep(8)
                            continue
                    break
            except Exception as e:
                print(f"⚠️ Error: {e}")
        await browser.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--retailers", type=str)
    args = parser.parse_args()
    lista = args.retailers.split(",") if args.retailers else None
    asyncio.run(ejecutar_bot_maestro(lista))