(function(){
    if(window.AssetPicker) return;

    function ensureStyles(){
        if(document.getElementById('asset-picker-style')) return;
        const style = document.createElement('style');
        style.id = 'asset-picker-style';
        style.textContent = `
            .asset-picker-backdrop{position:fixed;inset:0;z-index:80;background:rgba(15,23,42,.42);backdrop-filter:blur(12px);display:none;align-items:center;justify-content:center;padding:24px}
            .asset-picker-backdrop.open{display:flex}
            .asset-picker-panel{width:min(920px,100%);max-height:min(720px,90vh);background:#fff;border:1px solid #e8edf3;border-radius:28px;box-shadow:0 28px 80px rgba(15,23,42,.22);display:flex;flex-direction:column;overflow:hidden}
            .asset-picker-head{height:68px;display:flex;align-items:center;justify-content:space-between;padding:0 22px;border-bottom:1px solid #eef2f7}
            .asset-picker-title{font-size:12px;font-weight:900;letter-spacing:.24em;text-transform:uppercase;color:#111827;display:flex;align-items:center;gap:10px}
            .asset-picker-count{font-size:11px;font-weight:900;letter-spacing:.14em;color:#94a3b8}
            .asset-picker-actions{display:flex;gap:8px;align-items:center}
            .asset-picker-btn{height:36px;padding:0 13px;border-radius:999px;border:1px solid #e2e8f0;background:#fff;color:#334155;font-size:11px;font-weight:800;display:inline-flex;align-items:center;gap:7px;cursor:pointer}
            .asset-picker-btn.primary{background:#111827;color:#fff;border-color:#111827}
            .asset-picker-btn.active{background:#111827;color:#fff;border-color:#111827}
            .asset-picker-btn.danger{background:#dc2626;color:#fff;border-color:#dc2626}
            .asset-picker-btn:disabled{opacity:.45;pointer-events:none}
            .asset-picker-body{padding:18px;overflow:auto;background:#f8fafc}
            .asset-picker-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(132px,1fr));gap:12px}
            .asset-picker-item{position:relative;background:#fff;border:1px solid #e8edf3;border-radius:18px;overflow:hidden;text-align:left;transition:all .16s ease;cursor:pointer}
            .asset-picker-item:hover{transform:translateY(-2px);box-shadow:0 12px 28px rgba(15,23,42,.1);border-color:#cbd5e1}
            .asset-picker-item.selected{border-color:#111827;box-shadow:0 0 0 2px #111827}
            .asset-picker-thumb{width:100%;aspect-ratio:1/1;background:#eef2f7;object-fit:cover;display:block}
            .asset-picker-name{padding:9px 10px 11px;font-size:11px;font-weight:800;color:#475569;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
            .asset-picker-check{position:absolute;top:8px;left:8px;width:26px;height:26px;border-radius:999px;background:rgba(255,255,255,.94);border:1px solid #dbe3ed;display:none;align-items:center;justify-content:center;color:#94a3b8;font-size:14px;font-weight:900;line-height:1;z-index:2}
            .asset-picker-body.select-mode .asset-picker-check{display:flex}
            .asset-picker-item.selected .asset-picker-check{background:#111827;border-color:#111827;color:#fff}
            .asset-picker-del{position:absolute;top:8px;right:8px;width:26px;height:26px;border-radius:999px;background:rgba(255,255,255,.94);border:1px solid #dbe3ed;display:none;align-items:center;justify-content:center;color:#dc2626;z-index:2;cursor:pointer}
            .asset-picker-item:hover .asset-picker-del{display:flex}
            .asset-picker-del:hover{background:#fee2e2}
            .asset-picker-empty{padding:72px 20px;text-align:center;color:#94a3b8;font-size:12px;font-weight:800;letter-spacing:.12em;text-transform:uppercase}
        `;
        document.head.appendChild(style);
    }

    let items = [];
    let onSelect = null;
    let selectMode = false;
    const selected = new Set();

    const TRASH = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18M8 6V4h8v2M19 6l-1 14H6L5 6M10 11v6M14 11v6"/></svg>';

    function ensureModal(){
        ensureStyles();
        let modal = document.getElementById('assetPicker');
        if(modal) return modal;
        modal = document.createElement('div');
        modal.id = 'assetPicker';
        modal.className = 'asset-picker-backdrop';
        modal.innerHTML = `
            <div class="asset-picker-panel" onclick="event.stopPropagation()">
                <div class="asset-picker-head">
                    <div class="asset-picker-title">资产库<span class="asset-picker-count" id="assetPickerCount"></span></div>
                    <div class="asset-picker-actions">
                        <button type="button" class="asset-picker-btn" id="assetPickerSelectBtn">多选</button>
                        <button type="button" class="asset-picker-btn" id="assetPickerSelectAllBtn" style="display:none">全选</button>
                        <button type="button" class="asset-picker-btn danger" id="assetPickerDeleteBtn" style="display:none" disabled>删除选中</button>
                        <button type="button" class="asset-picker-btn" id="assetPickerUploadBtn">上传图片</button>
                        <button type="button" class="asset-picker-btn primary" id="assetPickerCloseBtn">关闭</button>
                        <input id="assetPickerFile" type="file" accept="image/*" multiple hidden>
                    </div>
                </div>
                <div class="asset-picker-body" id="assetPickerBody">
                    <div id="assetPickerGrid" class="asset-picker-grid"></div>
                </div>
            </div>`;
        document.body.appendChild(modal);
        modal.addEventListener('click', () => close());
        document.getElementById('assetPickerCloseBtn').onclick = close;
        document.getElementById('assetPickerSelectBtn').onclick = toggleSelectMode;
        document.getElementById('assetPickerSelectAllBtn').onclick = selectAll;
        document.getElementById('assetPickerDeleteBtn').onclick = deleteSelected;
        document.getElementById('assetPickerUploadBtn').onclick = () => document.getElementById('assetPickerFile').click();
        document.getElementById('assetPickerFile').onchange = async (event) => {
            const files = [...(event.target.files || [])];
            if(!files.length) return;
            const form = new FormData();
            files.forEach(file => form.append('files', file));
            await fetch('/api/assets/upload', {method:'POST', body:form});
            event.target.value = '';
            await load();
        };
        return modal;
    }

    function updateBulkUI(){
        const body = document.getElementById('assetPickerBody');
        if(!body) return;
        const selectBtn = document.getElementById('assetPickerSelectBtn');
        const allBtn = document.getElementById('assetPickerSelectAllBtn');
        const delBtn = document.getElementById('assetPickerDeleteBtn');
        const count = document.getElementById('assetPickerCount');
        body.classList.toggle('select-mode', selectMode);
        selectBtn.classList.toggle('active', selectMode);
        selectBtn.textContent = selectMode ? '退出多选' : '多选';
        allBtn.style.display = selectMode ? '' : 'none';
        delBtn.style.display = selectMode ? '' : 'none';
        delBtn.disabled = selected.size === 0;
        delBtn.textContent = selected.size ? `删除选中 (${selected.size})` : '删除选中';
        const allSel = items.length && items.every(it => selected.has(it.id));
        allBtn.textContent = allSel ? '取消全选' : '全选';
        count.textContent = selectMode && selected.size ? `${selected.size} / ${items.length}` : '';
    }

    function renderGrid(){
        const grid = document.getElementById('assetPickerGrid');
        if(!grid) return;
        if(!items.length){
            grid.innerHTML = '<div class="asset-picker-empty">暂无资产，先上传图片</div>';
            updateBulkUI();
            return;
        }
        grid.innerHTML = items.map(item => `
            <div class="asset-picker-item ${selected.has(item.id) ? 'selected' : ''}" data-id="${escapeAttr(item.id)}">
                <div class="asset-picker-check">${selected.has(item.id) ? '✓' : '+'}</div>
                <div class="asset-picker-del" data-del="${escapeAttr(item.id)}" title="删除">${TRASH}</div>
                <img class="asset-picker-thumb" src="${escapeAttr(item.url)}" alt="">
                <div class="asset-picker-name">${escapeHtml(item.name || 'image')}</div>
            </div>
        `).join('');
        grid.querySelectorAll('.asset-picker-item').forEach((el) => {
            const id = el.getAttribute('data-id');
            const item = items.find(it => it.id === id);
            el.onclick = () => {
                if(selectMode){ toggleSelect(id); return; }
                if(onSelect && item) onSelect(item);
                close();
            };
            const del = el.querySelector('.asset-picker-del');
            if(del) del.onclick = (ev) => { ev.stopPropagation(); deleteOne(id); };
        });
        updateBulkUI();
    }

    async function load(){
        const grid = document.getElementById('assetPickerGrid');
        grid.innerHTML = '<div class="asset-picker-empty">Loading</div>';
        const data = await fetch('/api/assets').then(r => r.json());
        items = data.items || [];
        for(const id of [...selected]) if(!items.some(it => it.id === id)) selected.delete(id);
        renderGrid();
    }

    function toggleSelectMode(){
        selectMode = !selectMode;
        if(!selectMode) selected.clear();
        renderGrid();
    }

    function toggleSelect(id){
        selected.has(id) ? selected.delete(id) : selected.add(id);
        renderGrid();
    }

    function selectAll(){
        const allSel = items.length && items.every(it => selected.has(it.id));
        if(allSel) selected.clear();
        else items.forEach(it => selected.add(it.id));
        renderGrid();
    }

    async function deleteOne(id){
        if(!confirm('删除这张资产？对应记录也会删除。')) return;
        await fetch(`/api/assets/${encodeURIComponent(id)}`, {method:'DELETE'});
        selected.delete(id);
        await load();
    }

    async function deleteSelected(){
        const ids = [...selected];
        if(!ids.length) return;
        if(!confirm(`删除 ${ids.length} 张资产？对应记录也会删除。`)) return;
        await fetch('/api/assets/bulk-delete', {
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({ids})
        });
        selected.clear();
        await load();
    }

    function open(callback){
        onSelect = callback;
        selectMode = false;
        selected.clear();
        const modal = ensureModal();
        modal.classList.add('open');
        load().catch(err => {
            document.getElementById('assetPickerGrid').innerHTML = `<div class="asset-picker-empty">${escapeHtml(err.message || '加载失败')}</div>`;
        });
    }

    function close(){
        const modal = document.getElementById('assetPicker');
        if(modal) modal.classList.remove('open');
    }

    function escapeHtml(str){ return String(str == null ? '' : str).replace(/[&<>"']/g, s => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s])); }
    const escapeAttr = escapeHtml;

    window.AssetPicker = {open, close};
})();
