#!/usr/bin/env python
# encoding: utf-8

#  _  _   _                _
# | |(_) | |__   __ _  ___| | ___   _ _ __
# | || | | '_ \ / _` |/ __| |/ / | | | '_ \
# | || | | |_) | (_| | (__|   <| |_| | |_) |
# |_|/ | |_.__/ \__,_|\___|_|\_\\__,_| .__/
#  |__/                              |_|
#
# (c) 2015, Alexander Saltanov <asd@mokote.com>
#

import glob
import json
import logging
import os
import re
import sys
import urllib2

from collections import defaultdict
from datetime import datetime
from tornado import template
from tornado.util import ObjectDict

from atomicfile import AtomicFile
from better_json import better_json_encode
from lj import LJServer, LJException


def ensure_list(val):
    if isinstance(val, (list, tuple)) or val is None:
        return val
    else:
        return [val]


def suggest_filename(line):
    def _cleanup_filename(s):
        return '_'.join(re.sub('[^a-zA-Z0-9/\.]', ' ', s).split())

    line = re.sub('\w+://', '', line)  # Drop protocols.
    line = re.sub('\?.*', '', line)  # Drop url params.
    line = re.sub('.jpeg', '.jpg', line)  # Replace ".jpeg" with ".jpg"

    # split line into valid keywords
    parts = line.split('/')

    # Drop sub-domains deeper than 2nd level
    # (e.g., "images.google.com" becomes "google.com", "static.farm.flickr.com" becomes "flickr.com")
    parts[0] = '.'.join(parts[0].split('.')[-2:])

    # Returns "domain.com/long_long_filename.jpg"
    return _cleanup_filename('%s/%s ' % (parts[0], parts[1]) + ' '.join(parts[2:]))


def extract_links(html):
    from HTMLParser import HTMLParser
    links = []

    class _extractor(HTMLParser):
        def _ensure_url(self, url):
            if url.startswith('//'):
                return 'http:' + url
            else:
                return url

        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            if tag == 'img':
                url = self._ensure_url(attrs['src'])
                links.append((tag, 'src', url, suggest_filename(url), 'image'))
            elif tag == 'a':
                if 'href' not in attrs:
                    return
                url = self._ensure_url(attrs['href'])
                if not url:
                    return
                url_lower = url.lower()
                for link_type, endings in (['music', ('.mp3', '.ogg')], ['image', ('.jpg', '.jpeg', '.png', '.gif')], ['document', ('.pdf', '.doc', '.txt')]):
                    for ext in endings:
                        if url_lower.endswith(ext):
                            links.append((tag, 'href', url, suggest_filename(url), link_type))
                            return

    extractor = _extractor()
    extractor.feed(html)
    return links


# Indexing entries
class BaseIndex(object):
    def __init__(self, storage):
        self.storage = storage
        self.rows = {}

    def put(self, entry):
        entry_id = entry['itemid']
        for key, title in self._entry_to_index_rows(entry):
            if key not in self.rows:
                self.rows[key] = {'key': key, 'title': title, 'entries': [entry_id]}
            else:
                self.rows[key]['entries'].append(entry_id)

    def data(self, sort=True):
        if sort:
            rows = self.sorted_rows()
        else:
            rows = self.rows.values()

        return {
            'meta': {
                'descriptor': self.descriptor(),
                'title': self.title(),
            },
            'rows': rows
        }

    def save(self):
        data = self.data()
        filename = '%s.js' % self.descriptor()
        logging.debug
        self.storage.ensure_dir(self.storage.json_index_path)
        self.storage.save_json(self.storage.json_index_path, filename, data)
        logging.debug('Saved index %s with %d rows.' % (filename, len(data['rows'])))

    def _entry_to_index_rows(self, entry):
        keys = ensure_list(self.entry_key(entry))
        if not keys:
            return []
        return [(self.row_key(k), self.row_title(k)) for k in keys]

    def descriptor(self):
        raise NotImplemented

    def title(self):
        return self.descriptor().capitalize()

    def entry_key(self, entry):
        raise NotImplemented

    def row_key(self, entry_key):
        return entry_key

    def row_title(self, entry_key):
        return self.row_key(entry_key)

    def sorted_rows(self):
        return sorted(self.rows.values(), key=lambda r: r['key'])


class TagIndex(BaseIndex):
    def descriptor(self):
        return 'tags'

    def entry_key(self, entry, add_special_tag_when_no_tag_found=False):
        if add_special_tag_when_no_tag_found:
            no_tags = ['no_tags']
        else:
            no_tags = None

        if not ('props' in entry and 'taglist' in entry['props']):
            return no_tags
        tags = entry['props']['taglist'].strip()
        if not tags:
            return no_tags
        return [t.strip() for t in tags.split(',')]

    def sorted_rows(self):
        return sorted(self.rows.values(), key=lambda r: len(r['entries']))


class MonthIndex(BaseIndex):
    def descriptor(self):
        return 'months'

    def entry_key(self, entry):
        if 'eventtime' not in entry:
            return None
        return datetime.strptime(entry['eventtime'], '%Y-%m-%d %H:%M:%S')

    def row_key(self, entry_key):
        return entry_key.strftime('%Y-%m')

    def row_title(self, entry_key):
        return entry_key.strftime('%B %Y')


# JournalStorage provides some simple interfaces to save journal data to JSON files and retrieve them back.
# I tried to keep the data as close to its original form as it's possible.
class JournalStorage(object):
    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.html_path = path
        self.media_path = os.path.abspath(os.path.join(self.path, 'media'))
        self.json_path = os.path.abspath(os.path.join(path, '_json'))
        self.json_entries_path = os.path.join(self.json_path, 'entries')
        self.json_comments_path = os.path.join(self.json_path, 'comments')
        self.json_index_path = os.path.join(self.json_path, 'indexes')

        self.max_comment_id = 0
        self.entries = {}
        self.comments = defaultdict(list)
        self.userinfo = {}
        self.usernames = {}
        self.indexes = {}
        self.modified = False

        for p in (self.json_entries_path, self.json_comments_path):
            self.ensure_dir(p)
        self.reload(force=True)

    def __contains__(self, entry_id):
        """if entry.id in journal..."""
        return entry_id in self.entries

    def ensure_dir(self, dir):
        if dir.startswith('/') and not dir.startswith(self.path):
            raise ValueError('Path should be either local or inside "%s", not "%s"' % (self.path, dir))
        if not dir.startswith('/'):
            dir = os.path.abspath(os.path.join(self.path, dir))
        if not os.path.exists(dir):
            os.makedirs(dir)

    def reload(self, force=False):
        if not self.modified and not force:
            logging.debug('Storage seems to be intact, skip reloading.')
            return

        logging.info('Loading data from %s...' % self.json_entries_path)
        self.entries = {}
        self.comments = defaultdict(list)

        if not os.path.exists(self.json_entries_path):
            return

        userinfo_path = os.path.join(self.json_path, 'userinfo.js')
        if os.path.exists(userinfo_path):
            with open(userinfo_path, 'r') as f:
                self.userinfo = json.load(f)
            logging.debug('Userinfo for "%s" preloaded' % self.userinfo['username'])

        usernames_path = os.path.join(self.json_path, 'usernames.js')
        if os.path.exists(usernames_path):
            with open(usernames_path, 'r') as f:
                self.usernames = json.load(f)
            logging.debug('%d stored usernames preloaded' % len(self.usernames))

        for fname in glob.iglob('%s/*.js' % self.json_entries_path):
            with open(fname, 'r') as f:
                data = json.load(f)
                self.entries[data['itemid']] = data
        logging.debug('%d stored entries preloaded' % len(self.entries))

        ncomments = 0
        for fname in glob.iglob('%s/*.js' % self.json_comments_path):
            with open(fname, 'r') as f:
                data = json.load(f)
                self.comments[data['entry_id']].append(data)
                self.max_comment_id = max(self.max_comment_id, data['comment_id'])
                ncomments += 1
        logging.debug('%d stored comments preloaded, max_comment_id = %d' % (ncomments, self.max_comment_id))

        for fname in glob.iglob('%s/*.js' % self.json_index_path):
            with open(fname, 'r') as f:
                data = json.load(f)
                self.indexes[data['meta']['descriptor']] = data
            logging.debug('%d stored rows preloaded from %s.js' % (len(data['rows']), data['meta']['descriptor']))

        self.modified = False

    def reindex(self):
        logging.info('Reindexing entries...')
        indexes = [cls(self) for cls in (MonthIndex, TagIndex)]
        for entry in self.entries.values():
            for idx in indexes:
                idx.put(entry)
        for idx in indexes:
            idx.save()

    def save_file(self, path, filename, data, mode='w'):
        filepath = os.path.join(path, filename)
        with AtomicFile(filepath, mode) as f:
            f.write(data)

    def save_json(self, path, filename, data, indent=True):
        self.save_file(path, filename, better_json_encode(data, indent))

    def add_entry(self, entry, **extras):
        # add extra fields
        for k, v in extras.items():
            entry[k] = v

        entry_id = entry['itemid']
        self.entries[entry_id] = entry
        self.save_json(self.json_entries_path, '%d.js' % entry_id, entry)
        self.modified = True

    def add_usernames(self, usermap):
        self.usernames.update(usermap)
        self.save_json(self.json_path, 'usernames.js', self.usernames, indent=True)
        self.modified = True

    def add_userinfo(self, userinfo):
        self.userinfo.update(userinfo)
        self.save_json(self.json_path, 'userinfo.js', self.userinfo, indent=True)
        self.modified = True

    def add_comment(self, comment, **extras):
        # add extra fields
        for k, v in extras.items():
            comment[k] = v

        entry_id = comment['entry_id']
        comment_id = comment['comment_id']

        self.comments[entry_id].append(comment)
        self.save_json(self.json_comments_path, '%d.js' % comment_id, comment)
        self.modified = True


# FeedBackup stores all the entries and comments available via LJ API into JSON files.
# Please note that the lastest available edition of the entry overwrites previous one - here is no versioning.
# FeedBackup never deletes entries or comments after the moment they have been fetched, so incremental backup is available right out of the box.
class FeedBackup(object):
    def __init__(self, storage):
        self.storage = storage

    # returns [(entry_id, time) ...]
    def list_entries(self, connection):
        items = []
        count = 0
        total = None
        last_sync = None

        while count != total:
            sync = connection.syncitems(last_sync)
            count = sync['count']
            total = sync['total']
            journalitems = [(int(e['item'][2:]), e['time']) for e in sync['syncitems'] if e['item'].startswith('L-')]
            if journalitems:
                items.extend(journalitems)
                last_sync = items[-1][1]
        return items

    def get_meta_since(self, highest, server, session):
        all = {'comments': {}, 'usermaps': {}}
        maxid = int(highest) + 1

        while highest < maxid:
            meta = server.fetch_comment_meta(highest, session)
            maxid = int(meta['maxid'])
            for id, data in meta['comments'].items():
                highest = max(int(id), int(highest))
                all['comments'][id] = data
            all['usermaps'].update(meta['usermaps'])
        all['maxid'] = maxid
        return all

    def get_bodies_since(self, highest, maxid, server, session):
        all = {}
        while int(highest) != int(maxid):
            meta = server.fetch_comment_bodies(highest, session)
            if not meta:
                break
            for id, data in meta.items():
                if int(id) > int(highest):
                    highest = id
                all[id] = data
            if maxid in meta:
                break
            logging.debug('Downloaded %d comments so far (highest=%d, maxid=%d)' % (len(all), highest, maxid))
        return all

    def _comment_parent_id(self, comment):
        try:
            return int(comment['parentid'])
        except ValueError:
            return 0
        raise RuntimeError("Must've never happened, but happened with comment %r" % comment)

    def backup(self, connection):
        # journal entries
        sync_list = self.list_entries(connection)
        downloaded = 0

        logging.info('%d entries listed' % len(sync_list))
        for entry_id, sync_time in sync_list:
            if entry_id in self.storage:
                entry = self.storage.entries[entry_id]
                if 'sync_time' in entry and entry['sync_time'] == sync_time:
                    continue

            logging.debug('syncing entry %s at %s' % (entry_id, sync_time))
            data = connection.getevents_one(entry_id)
            assert(len(data['events']) == 1)
            downloaded += 1
            self.storage.add_entry(data['events'][0], sync_time=sync_time)
        logging.info('%d entries downloaded' % downloaded)

        # comments
        session = connection.sessiongenerate()
        meta = self.get_meta_since(self.storage.max_comment_id, connection, session)
        self.storage.add_usernames(meta['usermaps'])
        logging.info('%d comments listed since comment_id %d' % (len(meta['comments']), self.storage.max_comment_id))

        if meta['comments']:
            bodies = self.get_bodies_since(self.storage.max_comment_id, meta['maxid'], connection, session)
            for k, v in sorted(bodies.items(), key=lambda c: self._comment_parent_id(c[1])):
                entry_id = int(v['jitemid'])
                parent_id = self._comment_parent_id(v)
                username = self.storage.usernames[v['posterid']] if v['posterid'] else ''
                logging.debug('fetching comment %d by "%s" for entry %d' % (int(k), username, entry_id))
                self.storage.add_comment(v, entry_id=entry_id, comment_id=int(k), comment_parent_id=parent_id, username=username)

        connection.sessionexpire(session)


# MediaBackup downloads images (img), audio (mp3, ogg), pdf
class MediaDownload(object):
    def __init__(self, storage):
        self.storage = storage
        self.status = []  # (status, url, filename)

    def _fetch_file(self, url, filename):
        path = os.path.join(self.storage.media_path, filename)
        if os.path.exists(path):
            logging.debug('Skipping %s' % url)
            return

        self.storage.ensure_dir(os.path.dirname(path))
        status_code = None

        try:
            r = urllib2.urlopen(url, timeout=10)
            status_code = r.getcode()
            data = r.read()
            self.storage.save_file(self.storage.media_path, filename, data, mode='wb')
            logging.debug('%s stored into %s' % (url, filename))
        except urllib2.HTTPError as err:
            status_code = err.code
            logging.debug('Failed to download %s: %d' % (url, status_code))
        except urllib2.URLError as err:
            status_code = 100
            logging.debug('Failed to download %s: %s' % (url, err))
        self.status.append((status_code, url, filename))

    def _parse_and_fetch(self, text):
        links = extract_links(text)
        if not links:
            return
        for l in links:
            tag, attr, url, filename, link_type = l
            self._fetch_file(url, filename)

    def download(self):
        if self.storage.userinfo and 'pickwurls' in self.storage.userinfo and self.storage.userinfo['pickwurls']:
            for url in self.storage.userinfo['pickwurls']:
                filename = suggest_filename(url)
                self._fetch_file(url, filename)

        for entry_id, entry in self.storage.entries.items():
            self._parse_and_fetch(entry['event'])

            if entry_id in self.storage.comments:
                for comment in self.storage.comments[entry_id]:
                    self._parse_and_fetch(comment['body'])

    def save_status(self):
        path = os.path.join(self.storage.media_path, 'status.txt')
        with AtomicFile(path, 'w') as f:
            for line in self.status:
                f.write('\t'.join([str(i) for i in line]))
                f.write('\n')


# An append-only comments tree, represents comments tree in a list of lists.
# It also generates HTML for comments.
class CommentsTree(object):
    def __init__(self, owner):
        self.tree = []
        self.deleted_comments = []
        self.owner = owner

    def find_place_by_parent_id(self, comment_id, sublevel=None):
        if comment_id == 0:
            return self.tree

        context = sublevel if sublevel is not None else self.tree
        comment_ids = [c.comment_id for c in context]

        if comment_id in comment_ids:
            n = comment_ids.index(comment_id)
            return context[n].comments
        else:
            for c in context:
                if c.comments:
                    place = self.find_place_by_parent_id(comment_id, sublevel=c.comments)
                    if place is not None:
                        return place

        # Sublevel search may end up with nothing
        if sublevel is not None:
            return None

        raise RuntimeError("Top-level search shouldn't fail")

    def add_comment(self, comment):
        comment['comments'] = []
        comment['_skip'] = False
        obj = ObjectDict(comment)
        place = self.find_place_by_parent_id(comment['comment_parent_id'])
        place.append(obj)
        if comment['state'] in ('D', 'B'):
            self.deleted_comments.append(obj)

    def mark_orphans(self):
        for c in sorted(self.deleted_comments, key=lambda x: x.comment_id, reverse=True):
            # Mark deleted comment when it has no subcomments or all its subcomments are skipped either.
            c._skip = len([x.comment_id for x in c.comments if not x._skip]) == 0

    def render(self, sublevel=None, n=0, indent=True):
        if sublevel is None:
            self.mark_orphans()
            comments = self.tree
        else:
            comments = sublevel

        indent_str = '\t' * n if indent else ''
        tmp = ['%s<ul>' % indent_str]
        for comment in comments:
            if comment._skip:
                continue

            comment['date'] = comment['date'].replace('T', ' ').replace('Z', '')
            comment['owner'] = 'owner' if comment['username'] == self.owner else 'guest'
            comment['indent'] = indent_str

            state = comment['state']
            if state:
                if state == 'D':
                    comment['state'] = 'deleted'
                elif state == 'B':
                    comment['state'] = 'banned'
                elif state == 'S':
                    comment['state'] = 'screened'
                elif state in ('A', 'F'):
                    comment['state'] = 'visible'
                else:
                    raise ValueError('Unknown comment state "%s" in comment %d' % (state, comment['comment_id']))
            else:
                comment['state'] = 'visible'

            tmp.append('%(indent)s<li data-comment-id="%(comment_id)s" data-parent-id="%(comment_parent_id)s">' % comment)
            tmp.append('%(indent)s<div class="comment comment--state-%(state)s comment--posted-by-%(owner)s"' % comment)

            if comment.username:
                tmp.append('%(indent)s<p class="comment__author"><a href="#" class="username">%(username)s</a></p>' % comment)
            else:
                tmp.append('%(indent)s<p class="comment__author"><em>Anonymous</em></p>' % comment)
            tmp.append('%(indent)s<p class="comment__date">%(date)s</p>' % comment)

            if comment.subject:
                tmp.append('%(indent)s<p class="comment__title">%(subject)s</p>' % comment)
            tmp.append('%(indent)s<p class="comment__text">%(body)s</p>' % comment)

            # TODO: по hover нужно подсвечивать весь тред вверх от того коммента, на котором стоит мышка.
            # Сначала можно сделать яваскриптом, а потом (если будет нужно), сделать прегенерацию тредов здесь.
            # Прадва, здесь придётся развернуть генерацию и сначала генерить глубокие уровни, а только потом собирать полученные thread_ids
            # и писать верхние уровни.
            if comment.comments:
                tmp.append(self.render(comment.comments, n + 1))

            tmp.append('%(indent)s</div>' % comment)
            tmp.append('%(indent)s</li>' % comment)

        tmp.append('%s</ul>' % indent_str)
        return '\n'.join(tmp)


# Entries -> HTML
class FeedRenderer(object):
    def __init__(self, storage):
        self.storage = storage
        self.templates_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'templates'))
        self.loader = template.Loader(self.templates_path)

        self.html_path = self.storage.html_path
        self.html_entries_path = os.path.abspath(os.path.join(self.html_path, 'entries'))
        self.html_indexes_path = os.path.abspath(os.path.join(self.html_path, 'indexes'))

        self.entries_meta = {}

    def _render(self, template_name, **kwargs):
        return self.loader.load(template_name).generate(**kwargs)

    def username(self):
        return self.storage.userinfo['username']

    def stats(self):
        info = ObjectDict(
            entries=len(self.storage.entries),
            comments=0,
            own_comments=0,
            other_comments=0
        )

        username = self.username()
        for entry_id, comments in self.storage.comments.items():
            info['comments'] += len(comments)
            for comment in comments:
                if comment['username'] == username:
                    info['own_comments'] += 1
                else:
                    info['other_comments'] += 1

        return info

    def generate_userinfo(self, filename=None):
        logging.debug('Rendering userinfo')

        userinfo = ObjectDict(self.storage.userinfo)
        username = self.username()
        stats = self.stats()

        html = self._render('userinfo.html', username=username, userinfo=userinfo, stats=stats)
        self.storage.save_file(self.storage.html_path, filename or 'userinfo.html', html)

    def generate_entries(self):
        logging.debug('Rendering entries')

        userinfo = ObjectDict(self.storage.userinfo)
        self.storage.ensure_dir(self.html_entries_path)

        for entry_id, entry in self.storage.entries.items():
            comments = self.storage.comments[entry_id]
            comments_tree = CommentsTree(userinfo.username)
            for comment in sorted(comments, key=lambda x: x['comment_id']):
                comments_tree.add_comment(comment)

            # ensure we have all basic entry attributes
            for attr in ('subject', 'props'):
                if attr not in entry:
                    entry[attr] = None

            # tags
            if 'taglist' in entry['props']:
                taglist = entry['props']['taglist'].strip()
                tags = [t.strip() for t in taglist.split(',')]
            else:
                tags = None

            meta = ObjectDict(
                entry_id=entry_id,
                comments=entry['reply_count'],
                image_links=0,
                music_links=0,
                document_links=0,
                title=entry['subject'],
                tags=tags,
                year=int(entry['eventtime'][:4]),
            )
            self.entries_meta[entry_id] = meta

            # remove <lj-cut>
            entry['event'] = re.sub('</?lj-cut[^>]*>', '', entry['event'])

            formatted = True if 'opt_preformatted' in entry['props'] and entry['props']['opt_preformatted'] == 1 else False
            if not formatted:
                tmp = []
                for line in entry['event'].splitlines(False):
                    tmp.append('%s<br>' % line)
                entry['event'] = '\n'.join(tmp)

            html = self._render('entry.html', entry=ObjectDict(entry), meta=meta, comments=comments_tree.render(), userinfo=userinfo)
            self.storage.save_file(self.html_entries_path, '%d.html' % entry['ditemid'], html)

    def render(self):
        logging.info('Rendering HTML...')
        # self.generate_userinfo()
        self.generate_entries()  # generate entries and fill entries_meta to be used later


def main(username, password, path):
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        connection = LJServer('backup.py; +https://github.com/sashka/lj_backup', 'Python-lj.py/0.0.1')
        try:
            userinfo = connection.login(username, password, getpickws=True, getpickwurls=True)
        except LJException as e:
            sys.exit(e)

        storage = JournalStorage(os.path.join(path, username))
        storage.add_userinfo(userinfo)

        feedbackup = FeedBackup(storage)
        feedbackup.backup(connection)

        storage.reload()
        storage.reindex()

        downloader = MediaDownload(storage)
        downloader.download()
        downloader.save_status()

        renderer = FeedRenderer(storage)
        renderer.render()


if __name__ == '__main__':
    main('lj_username', 'password', '.')
