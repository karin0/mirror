use git2::{BranchType, Error, FetchOptions, Oid, ProxyOptions, PushOptions, Remote, Repository};
use serde::Deserialize;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tiny_http::{Response, Server};

type Result<T> = std::result::Result<T, Error>;
type AnyResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Repo<'a> {
    repo: &'a Repository,
    proxy: Option<&'a str>,
}

impl<'a> Repo<'a> {
    fn set_remote(&self, name: &str, url: &str) -> Result<Remote> {
        if let Ok(r) = self.repo.remote(name, url) {
            Ok(r)
        } else {
            self.repo.remote_delete(name)?;
            self.repo.remote(name, url)
        }
    }

    fn proxy_opts(&self) -> Option<ProxyOptions> {
        self.proxy.as_ref().map(|p| {
            let mut r = ProxyOptions::new();
            r.url(p);
            r
        })
    }

    fn fetch_opts(&self) -> Option<FetchOptions> {
        self.proxy_opts().map(|p| {
            let mut r = FetchOptions::new();
            r.proxy_options(p);
            r
        })
    }

    fn push_opts(&self) -> Option<PushOptions> {
        self.proxy_opts().map(|p| {
            let mut r = PushOptions::new();
            r.proxy_options(p);
            r
        })
    }

    fn fetch(&self, remote: &mut Remote) -> Result<HashMap<String, Oid>> {
        remote.connect_auth(git2::Direction::Fetch, None, self.proxy_opts())?;
        let r = self.do_fetch(remote);
        if let Err(e) = remote.disconnect() {
            eprintln!("disconnect: {:?}", e);
        }
        r
    }

    fn do_fetch(&self, remote: &mut Remote) -> Result<HashMap<String, Oid>> {
        let mut specs = Vec::new();
        for s in remote.list()?.iter() {
            let name = s.name();
            if name.starts_with("refs/heads/") {
                specs.push(name.to_string());
            }
        }
        let mut res = HashMap::with_capacity(specs.len());
        for spec in specs {
            let specs: &[&str] = &[&spec];
            let mut opts = self.fetch_opts();
            remote.fetch(specs, opts.as_mut(), None)?;
            match self.repo.find_reference("FETCH_HEAD") {
                Ok(r) => {
                    let oid = r.target().unwrap();
                    let br = spec[11..].to_string();
                    res.insert(br, oid);
                }
                Err(e) => {
                    eprintln!("empty branch: {:?}", e);
                    continue;
                }
            }
        }
        if res.is_empty() {
            return Err(Error::from_str("empty remote"));
        }
        Ok(res)
    }
}

fn merge(repo: &Repository, branch: &str, oid: Option<Oid>) -> Result<()> {
    let ref_name = format!("refs/heads/{}", branch);
    match repo.find_reference(&ref_name) {
        Ok(mut r) => {
            if let Some(oid) = oid {
                r.set_target(oid, "mirror update")?;
            } else {
                r.delete()?;
            }
        }
        Err(_) => {
            if let Some(oid) = oid {
                repo.reference(&ref_name, oid, false, "mirror new")?;
            }
        }
    };
    Ok(())
}

#[derive(Debug, Copy, Clone)]
enum RefUpdate {
    Unchanged,
    A(Option<Oid>),
    B,
    Both,
    Conflict,
}

fn stat_remote<S: AsRef<str>, K: Iterator<Item = S>>(
    local: K,
    mut remote: HashMap<String, Oid>,
) -> HashMap<String, Option<Oid>> {
    let mut res = HashMap::with_capacity(local.size_hint().0);
    for br in local {
        let br = br.as_ref();
        res.insert(br.to_string(), remote.remove(br));
    }
    for (br, oid) in remote {
        res.insert(br, Some(oid));
    }
    res
}

fn work(path: &str, a_url: &str, b_url: &str, proxy: Option<&str>) -> Result<()> {
    let repo = Repository::init(path)?;
    let ctx = Repo { repo: &repo, proxy };
    let mut remote_a = ctx.set_remote("a", a_url)?;
    let mut a = ctx.fetch(&mut remote_a)?;

    let branches = repo.branches(Some(BranchType::Local))?;
    let mut status = HashMap::with_capacity(branches.size_hint().0);
    for br in branches {
        let br = br?.0;
        let oid = br.get().target().unwrap();
        let br = br.name()?.unwrap().to_string();
        if let Some(a_oid) = a.remove(&br) {
            if oid == a_oid {
                status.insert(br, (RefUpdate::Unchanged, Some(oid)));
            } else {
                status.insert(br, (RefUpdate::A(Some(oid)), Some(a_oid)));
            }
        } else {
            status.insert(br, (RefUpdate::A(Some(oid)), None));
        }
    }
    for (br, oid) in a {
        status.insert(br, (RefUpdate::A(None), Some(oid)));
    }

    let mut remote_b = ctx.set_remote("b", b_url)?;
    let b = ctx.fetch(&mut remote_b)?;

    let mut cnt_a = 0;
    let mut cnt_b = 0;
    let mut changed = false;
    for (br, b_now) in stat_remote(status.keys(), b) {
        if let Some((st, now)) = status.get_mut(&br) {
            match st {
                RefUpdate::Unchanged => {
                    if b_now == *now {
                        *st = RefUpdate::Unchanged;
                    } else {
                        *now = b_now;
                        *st = RefUpdate::B;
                        cnt_b += 1;
                        changed = true;
                    }
                }
                RefUpdate::A(old) => {
                    changed = true;
                    if b_now == *now {
                        *st = RefUpdate::Both;
                    } else if b_now == *old {
                        cnt_a += 1;
                    } else if let Some(a_now) = *now {
                        if let Some(b_now) = b_now {
                            if repo.graph_descendant_of(a_now, b_now)? {
                                eprintln!("{}: {}: ff to {} (A)", path, br, a_now);
                                cnt_a += 1;
                            } else if repo.graph_descendant_of(b_now, a_now)? {
                                eprintln!("{}: {}: ff to {} (B)", path, br, b_now);
                                *now = Some(b_now);
                                *st = RefUpdate::B;
                                cnt_b += 1;
                            } else {
                                if let Ok((ahead, behind)) = repo.graph_ahead_behind(b_now, a_now) {
                                    eprintln!(
                                        "{}: {}: {} ahead, {} behind",
                                        path, br, ahead, behind
                                    );
                                }
                                *st = RefUpdate::Conflict;
                            }
                        } else {
                            *st = RefUpdate::Conflict;
                        }
                    } else {
                        *st = RefUpdate::Conflict;
                    }
                }
                _ => unreachable!(),
            }
        } else {
            changed = true;
            status.insert(br, (RefUpdate::B, b_now));
            cnt_b += 1;
        }
    }

    if !changed {
        // eprintln!(
        //     "{}: {} branches unchanged: {}",
        //     path,
        //     status.len(),
        //     status
        //         .keys()
        //         .into_iter()
        //         .map(|x| x.as_ref())
        //         .collect::<Vec<&str>>()
        //         .join(", ")
        // );
        return Ok(());
    }

    let mut to_a = Vec::with_capacity(cnt_b);
    let mut to_b = Vec::with_capacity(cnt_a);
    for (br, (st, now)) in status {
        let delete = now.is_none();
        let push_to = |v: &mut Vec<String>| {
            let spec = if delete {
                format!(":refs/heads/{}", br)
            } else {
                format!("refs/heads/{}:refs/heads/{}", br, br)
            };
            v.push(spec);
        };
        let now_ref = || match &now {
            Some(oid) => oid.to_string(),
            None => "<deleted>".to_string(),
        };
        match st {
            RefUpdate::Unchanged => {
                eprintln!("{}: {}:{} unchanged", path, br, now_ref());
            }
            RefUpdate::A(_) => {
                merge(&repo, &br, now)?;
                eprintln!("{}: {}:{} -> B", path, br, now_ref());
                push_to(&mut to_b);
            }
            RefUpdate::B => {
                merge(&repo, &br, now)?;
                eprintln!("{}: {}:{} -> A", path, br, now_ref());
                push_to(&mut to_a);
            }
            RefUpdate::Both => {
                eprintln!("{}: {}:{} both changed", path, br, now_ref());
                merge(&repo, &br, now)?;
            }
            RefUpdate::Conflict => {
                eprintln!("{}: {}: conflict!", path, br);
            }
        }
    }

    let mut opts = ctx.push_opts();
    if !to_a.is_empty() {
        remote_a.push(&to_a, opts.as_mut())?;
    }
    if !to_b.is_empty() {
        remote_b.push(&to_b, opts.as_mut())?;
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
struct Conf {
    repos: HashMap<String, (String, String)>,
    proxy: Option<String>,
    hosts: Option<HashMap<String, String>>,
    bind: Option<String>,
}

impl Conf {
    fn read() -> AnyResult<Self> {
        let arg = std::env::args().nth(1);
        let conf = arg.as_deref().unwrap_or("mirror.yaml");
        Ok(serde_yaml::from_reader(std::fs::File::open(conf)?)?)
    }
}

fn main() -> AnyResult<()> {
    let conf = Conf::read()?;
    let hosts = conf.hosts;
    let parse_url = |url: &mut String| {
        if let Some(hosts) = &hosts {
            if url.starts_with('(') {
                if let Some(p) = url.find(')') {
                    let host = &url[1..p];
                    if let Some(host) = hosts.get(host) {
                        *url = format!("{}{}", host, &url[p + 1..]);
                    }
                }
            }
        }
    };
    let mut repos = conf.repos;
    for (_, (url_a, url_b)) in repos.iter_mut() {
        parse_url(url_a);
        parse_url(url_b);
    }
    drop(hosts);

    let arc = Arc::new(Mutex::new((repos, conf.proxy)));
    let app = arc.clone();
    let worker = thread::spawn(move || loop {
        let app = app.lock().unwrap();
        let (repos, proxy) = app.deref();
        for (path, (url_a, url_b)) in repos {
            if let Err(e) = work(path, url_a, url_b, proxy.as_deref()) {
                eprintln!("{}: {}", path, e);
            }
        }
        drop(app);
        thread::sleep(Duration::from_secs(60));
    });
    if let Some(addr) = conf.bind {
        let app = arc;
        let server = if let Some(unix) = addr.strip_prefix("unix:") {
            Server::http_unix(unix.as_ref()).unwrap()
        } else {
            Server::http(addr).unwrap()
        };
        eprintln!("Listening on {:?}", server.server_addr());
        for request in server.incoming_requests() {
            let method = request.method();
            let url = request.url();
            let path = &url[1..];
            let app = app.lock().unwrap();
            let (repos, proxy) = app.deref();
            if let Some((url_a, url_b)) = repos.get(path) {
                if let Err(e) = work(path, url_a, url_b, proxy.as_deref()) {
                    eprintln!("{} {}: {}", method, url, e);
                } else {
                    eprintln!("{} {}: ok", method, url);
                }
            } else {
                eprintln!("{} {}: not found", method, url);
            }
            drop(app);
            if let Err(e) = request.respond(Response::from_data("ok")) {
                eprintln!("respond: {}", e);
            }
        }
    } else {
        worker.join().unwrap();
    }
    Ok(())
}
