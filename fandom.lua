dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local item_type = nil
local item_name = nil
local item_value = nil
local item_wiki = nil

local selftext = nil

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false

local outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(item)
  discovered_items[item] = true
end

allowed = function(url, parenturl)
  if string.match(url, "[%?&]mobileaction=")
    or string.match(url, "/wiki/Special:[A-Za-z0-9]+[^A-Za-z0-9]")
    or string.match(url, "/wiki/[^%?&]+%?.*oldid=")
    or string.match(url, "/wiki/[^%?&]+%?.*diff=")
    or string.match(url, "/wiki/[^%?&]+%?.*dir=")
    or string.match(url, "/wiki/[^%?&]+%?.*offset=")
    or string.match(url, "/wiki/[^%?&]+%?.*action=edit")
    or string.match(url, "/wiki/[^%?&]+%?.*action=history")
    or string.match(url, "^https?://[^/]*fandom%.com/signin")
    or string.match(url, "^https?://[^/]*fandom%.com/register") then
    return false
  end

  if not (
    string.match(url, "^https?://[^/]*fandom%.com")
    or string.match(url, "^https?://[^/]*wikia%.org")
    or string.match(url, "^https?://[^/]*wikia%.com")
    or string.match(url, "^https?://[^/]*nocookie%.net/")
  ) then
    local temp = ""
    for c in string.gmatch(url, "(.)") do
      local b = string.byte(c)
      if b < 32 or b > 126 then
        c = string.format("%%%02X", b)
      end
      temp = temp .. c
    end
    outlinks[string.match(temp, "^([^%s]+)")] = true
    return false
  end

  local user = string.match(url, "/wiki/User:([^%?&]+)")
  if user then
    discover_item("user:" .. item_wiki .. ":" .. user)
  end

  local fandom_wiki = string.match(url, "^https?://([^%.]+)%.fandom%.com/")
  if fandom_wiki and fandom_wiki ~= item_wiki then
    return false
  end

  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if tested[s] == nil then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end

  if string.match(url, "^https://[^%.]+.wikia.nocookie.net/") then
    if item_type == "url" then
      return true
    end
    discover_item("url:" .. url)
    return false
  end

  local wiki_page = string.match(url, "/wiki/([^%?&]+)")
  if wiki_page and (
    ids[urlparse.unescape(wiki_page)]
    or (item_type == "base" and string.match(wiki_page, "^Special:"))
  ) then
    return true
  end

  if item_type == "base" and (
    string.match(url, "api%.php%?.*action=query")
    or string.match(url, "api%.php$")
    or string.match(url, "^https?://[^/]+/f$")
    or string.match(url, "controller=DiscussionThread")
  ) then
    return true
  end

  if item_type == "page" and string.match(url, "controller=ArticleCommentsController") then
    return true
  end

  if string.match(url, "controller=Fandom")
    or string.match(url, "controller=RecirculationApi")
    or string.match(url, "controller=Lightbox") then
    return true
  end

  if item_type == "f" or item_type == "page" then
    for s in string.gmatch(url, "([0-9]+)") do
      if ids[s] then
        return true
      end
    end
  end
  
  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if item_type == "url" then
    return false
  end

  if not processed(url) and allowed(url, parent["url"]) then
    addedtolist[url] = true
    return true
  end
  
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  if item_type == "url" then
    return urls
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function check(newurl)
    if string.match(newurl, "^https?://[^%.]+%.fandom%.com")
      and not string.match(newurl, "%.php") then
      check(string.gsub(newurl, "^(https?://[^%.]+%.)fandom%.com", "%1wikia%.org"))
      check(string.gsub(newurl, "^(https?://[^%.]+%.)fandom%.com", "%1wikia%.com"))
    end
    if string.match(newurl, "%%22") then
      return nil
    end
    newurl = decode_codepoint(newurl)
    local origurl = url
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and string.match(url_, "^https?://[^/%.]+%..+")
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  if allowed(url) and status_code < 300 and not string.match(url, "^https://[^%.]+.wikia.nocookie.net/") then
    html = read_file(file)
    if string.match(url, "/api%.php%?action=query&pageids=[0-9]+&format=json$") then
      local title = JSON:decode(html)["query"]["pages"][item_value]["title"]
      if not title then
        return urls
      end
      local base = string.match(url, "^(https?://[^/]+/)")
      check(base .. "index.php?curid=" .. item_value)
      ids[title] = true
      title = urlparse.escape(title)
      ids[title] = true
      check(base .. "wiki/" .. title)
      check(base .. "wikia.php?controller=ArticleCommentsController&method=getCommentCount&namespace=0&title=" .. title .. "&hideDeleted=true")
      check(base .. "wikia.php?controller=ArticleCommentsController&method=getComments&title=" .. title .. "&namespace=0&hideDeleted=true")
      title = string.gsub(title, "%%20", "%+")
      ids[title] = true
      check(base .. "wiki/" .. title)
      check(base .. "wikia.php?controller=ArticleCommentsController&method=getCommentCount&namespace=0&title=" .. title .. "&hideDeleted=true")
      check(base .. "wikia.php?controller=ArticleCommentsController&method=getComments&title=" .. title .. "&namespace=0&hideDeleted=true")
    end
    if string.match(url, "^https?://[^/]*/wiki/[^%?&]") then
      local data = string.match(html, "<script>var%s+_plc%s*=%s*({.-});[^<]*</script>")
      if data then
        data = JSON:decode(data)
        if data["pgId"] == item_value
          and not string.match(url, "[%?&]file=") then
          for image_key in string.gmatch(html, 'data%-image%-key="([^"]+)') do
            local newurl = url
            local base = string.match(url, "^(https?://[^/]+/)")
            if string.match(url, "%?") then
              newurl = newurl .. "&"
            else
              newurl = newurl .. "?"
            end
            newurl = newurl .. "file=" .. image_key
            check(newurl)
            check(base .. "wikia.php?controller=Lightbox&method=getMediaDetail&fileTitle=" .. image_key .. "&format=json")
          end
        end
      end
    end
    if string.match(url, "^https?://[^/]+/api%.php$") then
      for _, name in pairs({"allpages", "allimages", "allinfoboxes"}) do
        check(url .. "?action=query&list=" .. name .. "&format=json")
      end
      local base = string.match(url, "^(https?://[^/]+/)")
      check(base .. "wiki/Special:SpecialPages")
      check(base .. "f")
      check(base .. "wikia.php?controller=RecirculationApi&method=getFandomArticles&limit=9")
      check(base .. "wikia.php?controller=RecirculationApi&method=getLatestThreads&_lang=en")
      check(base .. "wikia.php?controller=Lightbox&method=lightboxModalContent&format=html&lightboxVersion=&userLang=en")
      check(base .. "wikia.php?controller=Lightbox&method=getTotalWikiImages&count=0&format=json&inclusive=true")
      check(base .. "wikia.php?controller=Fandom%5CFandomDesktop%5CRail%5CRailController&method=renderLazyContentsAnon&modules%5B%5D=Fandom%5CFandomDesktop%5CRail%5CPopularPagesModuleService&fdRightRail=&uselang=&useskin=fandomdesktop:")
      check(base)
    end
    if string.match(url, "/api%.php%?.*action=query") then
      local json = JSON:decode(html)
      if json["continue"] then
        for key, value in pairs(json["continue"]) do
          if key ~= "continue" then
            value = string.gsub(urlparse.escape(value), "%%", "%%%%")
            if string.match(url, "[%?&]" .. key .. "=") then
              check(string.gsub(url, "([%?&]" .. key .. "=)[^%?&]+", "%1" .. value))
            else
              check(string.gsub(url, "(&format=json)", "&" .. key .. "=" .. value .. "%1"))
            end
          end
        end
      end
      local largest = 0
      for _, pattern in pairs({'"pageid"%s*:%s*"?([0-9]+)"?', "[%?&]curid=([0-9]+)"}) do
        for pageid in string.gmatch(html, pattern) do
          pageid = tonumber(pageid)
          if pageid > largest then
            largest = pageid
          end
        end
      end
      for i=0,largest do
        discover_item("page:" .. item_wiki .. ":" .. tostring(i))
      end
    end
    if string.match(url, "^/f/p/[0-9]+$") then
      check("https://lgbta.fandom.com/wikia.php?limit=6&sortBy=trending&responseGroup=small&viewableOnly=true&excludedThreads=" .. item_value .. "&controller=DiscussionThread&method=getThreads")
    end
    if string.match(url, "/wikia%.php%?.*controller=DiscussionThread") then
      local json = JSON:decode(html)
      if item_type == "f" then
        for _, data in pairs(json["_embedded"]["doc:posts"]) do
          ids[data["id"]] = true
          check("https://lgbta.fandom.com/f/p/" .. item_value .. "/r/" .. data["id"])
        end
      elseif item_type == "base" then
        for _, data in pairs(json["_embedded"]["threads"]) do
          discover_item("f:" .. item_wiki .. ":" .. data["id"])
        end
      end
    end
    if string.match(url, "wikia%.php") then
      for user in string.gmatch(html, '"name"%s*:%s*"([^"]+)"') do
        discover_item("user:" .. item_wiki .. ":" .. user)
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()

  local wiki = string.match(url["url"], "^https?://([^%.]+)%.fandom%.com/api%.php$")
  local type_ = nil
  if wiki then
    type_ = "base"
    value = nil
  end
  if not wiki then
    wiki, value = string.match(url["url"], "^https?://([^%.]+)%.fandom%.com/api%.php%?action=query&pageids=([0-9]+)&format=json$")
    type_ = "page"
  end
  if not wiki then
    wiki, value = string.match(url["url"], "^https?://([^%.]+)%.fandom%.com/f/p/([0-9]+)$")
    type_ = "f"
  end
  if not wiki and string.match(url["url"], "^https://[^%.]+.wikia.nocookie.net/")then
    wiki = url["url"]
    type_ = "url"
  end
  if wiki then
    abortgrab = false
    item_type = type_
    item_wiki = wiki
    item_value = value
    item_name = item_type .. ":" .. item_wiki
    if item_value then
      ids[item_value] = true
      item_name = item_name .. ":" .. item_value
    end
    print("Archiving item " .. item_name)
  end

  if status_code == 204 then
    return wget.actions.EXIT
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if string.match(url["url"], "/wiki/")
      and string.match(newloc, "/wiki/") then
      ids[string.match(newloc, "/wiki/([^%?&]+)")] = true
    end
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if (status_code >= 200 and status_code <= 399) then
    downloaded[url["url"]] = true
    downloaded[string.gsub(url["url"], "https?://", "http://")] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end
  
  if status_code >= 500
      or (status_code >= 400 and status_code ~= 404)
      or status_code  == 0 then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 8
    if not allowed(url["url"]) then
        maxtries = 0
    end
    if tries >= maxtries then
      io.stdout:write("\nI give up...\n")
      io.stdout:flush()
      tries = 0
      if allowed(url["url"]) then
        return wget.actions.ABORT
      else
        return wget.actions.EXIT
      end
    end
    os.execute("sleep " .. math.floor(math.pow(2, tries)))
    tries = tries + 1
    return wget.actions.CONTINUE
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["fandom-yeogkicq6a1f1cs"] = discovered_items,
    ["urls-nbv5q13ie6jf4br"] = outlinks
  }) do
    local items = nil
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
    end
    if items ~= nil then
      local tries = 0
      while tries < 10 do
        local body, code, headers, status = http.request(
          "http://blackbird-amqp.meo.ws:23038/" .. key .. "/",
          items
        )
        if code == 200 or code == 409 then
          break
        end
        io.stdout:write("Could not queue items.\n")
        io.stdout:flush()
        os.execute("sleep " .. math.floor(math.pow(2, tries)))
        tries = tries + 1
      end
      if tries == 10 then
        abort_item()
      end
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab then
    abort_item()
  end
  return exit_status
end
