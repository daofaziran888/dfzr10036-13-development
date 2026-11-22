import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import SnapshotMaintenanceView from '@/views/SnapshotMaintenanceView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    {
      path: '/snapshot-maintenance',
      name: 'snapshot-maintenance',
      component: SnapshotMaintenanceView,
    },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
