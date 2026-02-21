module ChartinkNativeScraper

using ChromeDevToolsLite
using Dates
using CSV
using DataFrames

export ScraperConfig, run_scraper!

# ==============================================================================
# 1. 🧱 CONFIGURATION
# ==============================================================================
Base.@kwdef struct ScraperConfig
    target_urls::Vector{String} = [
        "https://chartink.com/dashboard/208896",  # Stocks/Sectors
        "https://chartink.com/dashboard/419640"   # Market Condition
    ]
    output_root::String = abspath("chartink_data")
    tmp_download_dir::String = abspath("chartink_tmp_downloads")
    nav_sleep_sec::Int = 8
    download_timeout_sec::Int = 30
end

# ==============================================================================
# 2. 🌐 CDP & BROWSER INTERACTION
# ==============================================================================
function enable_headless_downloads!(page, download_dir::String)
    @info "🔓 Configuring CDP to allow headless downloads..."
    mkpath(download_dir)
    ChromeDevToolsLite.execute(page, "Browser.setDownloadBehavior", Dict(
        "behavior" => "allow",
        "downloadPath" => download_dir,
        "eventsEnabled" => true
    ))
end

function get_dashboard_name(page)::String
    raw_title = ChromeDevToolsLite.evaluate(page, "document.title") 
    val = isa(raw_title, Dict) && haskey(raw_title, "value") ? raw_title["value"] : "Unknown_Dashboard"
    
    clean_title = replace(val, " - Chartink.com" => "") |> 
                  x -> replace(x, " - Chartink" => "") |> 
                  x -> replace(x, r"[^a-zA-Z0-9 \-_]" => "") |> 
                  x -> replace(strip(x), " " => "_")
    return isempty(clean_title) ? "Dashboard_Unknown" : clean_title
end

# ==============================================================================
# 3. 🧠 SCHEMA FINGERPRINTING & TRIGGERING
# ==============================================================================
function extract_schema_map(page)::Dict{String, String}
    js_payload = """
    (() => {
        let schemaMap = {};
        document.querySelectorAll("div.card").forEach(card => {
            let titleEl = card.querySelector(".card-header, h1, h2, h3, h4, h5, h6");
            let name = titleEl ? titleEl.innerText.trim() : "Unknown_Widget";
            let cleanName = name.replace(/[^a-zA-Z0-9]/g, "_").substring(0, 50);
            
            let table = card.querySelector("table");
            if (!table) return;
            
            let headers = Array.from(table.querySelectorAll("th")).map(th => 
                th.innerText.replace(/Sort table by.*/i, "").replace(/\\n/g, " ").trim()
            ).join(",");
            
            if (headers.length > 0) schemaMap[headers] = cleanName;
        });
        return schemaMap;
    })()
    """
    res = ChromeDevToolsLite.evaluate(page, js_payload)
    raw_map = isa(res, Dict) && haskey(res, "value") ? res["value"] : Dict{String, Any}()
    return Dict{String, String}(string(k) => string(v) for (k, v) in raw_map)
end

function trigger_native_downloads!(page)::Int
    js_click = """
    (() => {
        const buttons = document.querySelectorAll('.buttons-csv, .buttons-excel, a.buttons-html5');
        let count = 0;
        buttons.forEach(btn => { btn.click(); count++; });
        return count;
    })()
    """
    res = ChromeDevToolsLite.evaluate(page, js_click)
    return isa(res, Dict) && haskey(res, "value") ? res["value"] : 0
end

# ==============================================================================
# 4. 🗂️ I/O SYNCHRONIZATION & RENAMING
# ==============================================================================
function wait_for_downloads(download_dir::String, initial_count::Int, expected_new::Int, timeout::Int)
    @info "  ⏳ Waiting for $expected_new files to flush to disk..."
    start_time = time()
    while time() - start_time < timeout
        current_files = readdir(download_dir)
        is_downloading = any(f -> endswith(f, ".crdownload") || endswith(f, ".tmp"), current_files)
        
        if !is_downloading && length(current_files) >= (initial_count + expected_new)
            return true
        end
        sleep(0.5)
    end
    @warn "  ⚠️ Download wait timed out!"
    return false
end

function fingerprint_and_route!(config::ScraperConfig, schema_map::Dict{String, String}, dashboard_name::String)
    files = filter(f -> endswith(f, ".csv") || endswith(f, ".xlsx"), readdir(config.tmp_download_dir, join=true))
    
    target_folder = joinpath(config.output_root, dashboard_name)
    mkpath(target_folder)

    for file_path in files
        try
            csv_headers = String.(CSV.File(file_path, limit=1).names)
            signature = join(csv_headers, ",")
            
            if haskey(schema_map, signature)
                target_name = schema_map[signature] * ".csv"
                target_path = joinpath(target_folder, target_name)
                
                mv(file_path, target_path, force=true)
                @info "  ✅ Processed: $(schema_map[signature])"
            else
                @warn "  ❓ Unmatched Schema in downloaded file: $signature"
                rm(file_path, force=true) 
            end
        catch e
            @error "Failed to process $file_path" exception=(e, catch_backtrace())
        end
    end
end

# ==============================================================================
# 5. 🚀 MAIN ORCHESTRATOR
# ==============================================================================
function run_scraper!(config::ScraperConfig = ScraperConfig())
    rm(config.tmp_download_dir, force=true, recursive=true)
    mkpath(config.tmp_download_dir)

    try
        @info "🔌 Connecting to Chrome..."
        page = ChromeDevToolsLite.connect_browser()
        enable_headless_downloads!(page, config.tmp_download_dir)

        for url in config.target_urls
            @info "🧭 Navigating to: $url"
            retry_nav = retry(() -> ChromeDevToolsLite.goto(page, url), delays=[2.0, 5.0, 10.0])
            retry_nav()
            
            sleep(config.nav_sleep_sec)
            
            dashboard_name = get_dashboard_name(page)
            @info "🏷️ Identified Dashboard: $dashboard_name"

            schema_map = extract_schema_map(page)
            initial_count = length(readdir(config.tmp_download_dir))
            
            expected_files = trigger_native_downloads!(page)
            
            if expected_files > 0
                wait_for_downloads(config.tmp_download_dir, initial_count, expected_files, config.download_timeout_sec)
                fingerprint_and_route!(config, schema_map, dashboard_name)
            else
                @warn "⚠️ No download buttons found on $url"
            end
        end
        
        @info "🎉 Scrape Cycle Complete. Data safely stored in: $(config.output_root)"
    catch e
        @error "Pipeline Crash" exception=(e, catch_backtrace())
    finally
        rm(config.tmp_download_dir, force=true, recursive=true)
    end
end

end # module

if abspath(PROGRAM_FILE) == @__FILE__
    ChartinkNativeScraper.run_scraper!()
end
